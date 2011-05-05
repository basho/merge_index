%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_segment).
-author("Rusty Klophaus <rusty@basho.com>").
-export([
    exists/1,
    open_read/1,
    open_write/1,
    filename/1,
    filesize/1,
    delete/1,
    data_file/1,
    offsets_file/1,
    from_buffer/2,
    from_iterator/2,
    info/4,
    iterator/1,
    iterator/4,
    iterators/6
]).

-include("merge_index.hrl").

-include_lib("kernel/include/file.hrl").
-define(BLOCK_SIZE, 65536).
-define(BLOOM_CAPACITY, 512).
-define(BLOOM_ERROR, 0.01).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.


exists(Root) ->
    filelib:is_file(data_file(Root)).

%% Create and return a new segment structure.
open_read(Root) ->
    %% Create the file if it doesn't exist...
    DataFileExists = filelib:is_file(data_file(Root)),
    case DataFileExists of
        true  ->
            %% Get the fileinfo...
            {ok, FileInfo} = file:read_file_info(data_file(Root)),

            OffsetsTable = read_offsets(Root),
            #segment {
                       root=Root,
                       offsets_table=OffsetsTable,
                       size = FileInfo#file_info.size
                     };
        false ->
            throw({?MODULE, missing__file, Root})
    end.

open_write(Root) ->
    %% Create the file if it doesn't exist...
    DataFileExists = filelib:is_file(data_file(Root)),
    OffsetsFileExists = filelib:is_file(offsets_file(Root)),
    case DataFileExists orelse OffsetsFileExists of
        true  ->
            throw({?MODULE, segment_already_exists, Root});
        false ->
            %% TODO: Do we really need to go through the trouble of writing empty files here?
            file:write_file(data_file(Root), <<"">>),
            file:write_file(offsets_file(Root), <<"">>),
            #segment {
                       root = Root,     
                       offsets_table = ets:new(segment_offsets, [ordered_set, public])
                     }
    end.

filename(Segment) ->
    Segment#segment.root.

filesize(Segment) ->
    Segment#segment.size.

delete(Segment) ->
    [ok = file:delete(X) || X <- filelib:wildcard(Segment#segment.root ++ ".*")],
    ets:delete(Segment#segment.offsets_table),
    ok.

%% Create a segment from a Buffer (see mi_buffer.erl)
from_buffer(Buffer, Segment) ->
    %% Open the iterator...
    Iterator = mi_buffer:iterator(Buffer),
    mi_segment_writer:from_iterator(Iterator, Segment).

from_iterator(Iterator, Segment) ->
    mi_segment_writer:from_iterator(Iterator, Segment).

%% Return the number of results under this IFT.
info(Index, Field, Term, Segment) ->
    Key = {Index, Field, Term},
    case get_offset_entry(Key, Segment) of
        {OffsetEntryKey, {_, Bloom, _, KeyInfoList}} ->
            case mi_bloom:is_element(Key, Bloom) of
                true  -> 
                    {_, _, OffsetEntryTerm} = OffsetEntryKey,
                    EditSig = mi_utils:edit_signature(OffsetEntryTerm, Term),
                    HashSig = mi_utils:hash_signature(Term),
                    F = fun({EditSig2, HashSig2, _, _, Count}, Acc) ->
                                case EditSig == EditSig2 andalso HashSig == HashSig2 of
                                    true ->
                                        Acc + Count;
                                    false -> 
                                        Acc
                                end
                        end,
                    lists:foldl(F, 0, KeyInfoList);
                false -> 
                    0
            end;
        _ ->
            0
    end.


%% iterator/1 - Create an iterator over the entire segment.
iterator(Segment) ->
    %% Check if the segment is small enough such that we want to read
    %% the entire thing into memory.
    {ok, FullReadSize} = application:get_env(merge_index, segment_full_read_size),
    case filesize(Segment) =< FullReadSize of
        true ->
            %% Read the entire segment into memory.
            {ok, Bytes} = file:read_file(data_file(Segment)),
            fun() -> iterate_all_bytes(undefined, Bytes) end;
        false ->
            %% Open a filehandle to the start of the segment.
            {ok, ReadAheadSize} = application:get_env(merge_index, segment_compact_read_ahead_size),
            {ok, FH} = file:open(data_file(Segment), [read, raw, binary, {read_ahead, ReadAheadSize}]),
            fun() -> iterate_all_filehandle(FH, undefined, undefined) end
    end.

%% @private Create an iterator over a binary which represents the
%% entire segment.
iterate_all_bytes(LastKey, <<1:1/integer, Size:15/unsigned-integer, Bytes:Size/binary, Rest/binary>>) ->
    Key = expand_key(LastKey, binary_to_term(Bytes)),
    iterate_all_bytes(Key, Rest);
iterate_all_bytes(Key, <<0:1/integer, Size:31/unsigned-integer, Bytes:Size/binary, Rest/binary>>) ->
    Results = binary_to_term(Bytes),
    iterate_all_bytes_1(Key, Results, Rest);
iterate_all_bytes(_, <<>>) ->
    eof.
iterate_all_bytes_1(Key, [Result|Results], Rest) ->
    {I,F,T} = Key,
    {V,K,P} = Result,
    {{I,F,T,V,K,P}, fun() -> iterate_all_bytes_1(Key, Results, Rest) end};
iterate_all_bytes_1(Key, [], Rest) ->
    iterate_all_bytes(Key, Rest).
    
%% @private Create an iterator over a filehandle starting at position
%% 0 of the segment.
iterate_all_filehandle(File, BaseKey, {key, ShrunkenKey}) ->
    CurrKey = expand_key(BaseKey, ShrunkenKey),
    {I,F,T} = CurrKey,
    Transform = fun({V,K,P}) -> {I,F,T,V,K,P} end,
    WhenDone = fun(NextEntry) -> iterate_all_filehandle(File, CurrKey, NextEntry) end,
    iterate_by_term_values(File, Transform, WhenDone);
iterate_all_filehandle(File, BaseKey, undefined) ->
    iterate_all_filehandle(File, BaseKey, read_seg_entry(File));
iterate_all_filehandle(File, _, eof) ->
    file:close(File),
    eof.


%%% Create an iterater over a single Term.
iterator(Index, Field, Term, Segment) ->
    %% Find the Key containing the offset information we need.
    Key = {Index, Field, Term},
    case get_offset_entry(Key, Segment) of
        {OffsetEntryKey, {BlockStart, Bloom, _LongestPrefix, KeyInfoList}} ->
            %% If we're aiming for an exact match, then check the
            %% bloom filter.
            case mi_bloom:is_element(Key, Bloom) of
                true -> 
                    {_, _, OffsetEntryTerm} = OffsetEntryKey,
                    EditSig = mi_utils:edit_signature(OffsetEntryTerm, Term),
                    HashSig = mi_utils:hash_signature(Term),
                    iterate_by_keyinfo(OffsetEntryKey, Key, EditSig, HashSig, BlockStart, KeyInfoList, Segment);
                false ->
                    fun() -> eof end
            end;
        undefined ->
            fun() -> eof end
    end.

%% Use the provided KeyInfo list to skip over terms that don't match
%% based on the edit signature. Clauses are ordered for most common
%% paths first.
iterate_by_keyinfo(BaseKey, Key, EditSigA, HashSigA, FileOffset, [Match={EditSigB, HashSigB, KeySize, ValuesSize, _}|Rest], Segment) ->
    %% In order to consider this a match, both the edit signature AND the hash signature must match.
    case EditSigA /= EditSigB orelse HashSigA /= HashSigB of
        true ->
            iterate_by_keyinfo(BaseKey, Key, EditSigA, HashSigA, FileOffset + KeySize + ValuesSize, Rest, Segment);
        false ->
            {ok, ReadAheadSize} = application:get_env(merge_index, segment_query_read_ahead_size),
            {ok, FH} = file:open(data_file(Segment), [read, raw, binary, {read_ahead, ReadAheadSize}]),
            file:position(FH, FileOffset),
            iterate_by_term(FH, BaseKey, [Match|Rest], Key)
    end;
iterate_by_keyinfo(_, _, _, _, _, [], _) ->
    fun() -> eof end.

%% Iterate over the segment file until we find the start of the values
%% section we want.
iterate_by_term(File, BaseKey, [{_, _, _, ValuesSize, _}|KeyInfoList], Key) ->
    %% Read the next entry in the segment file.  Value should be a
    %% key, otherwise error. 
    case read_seg_entry(File) of
        {key, ShrunkenKey} ->
            CurrKey = expand_key(BaseKey, ShrunkenKey),
            %% If the key is smaller than the one we need, keep
            %% jumping. If it's the one we need, then iterate
            %% values. Otherwise, it's too big, so close the file and
            %% return.
            if 
                CurrKey < Key ->
                    file:read(File, ValuesSize),
                    iterate_by_term(File, CurrKey, KeyInfoList, Key);
                CurrKey == Key ->
                    Transform = fun(Value) -> Value end,
                    WhenDone = fun(_) -> file:close(File), eof end,
                    fun() -> iterate_by_term_values(File, Transform, WhenDone) end;
                CurrKey > Key ->
                    file:close(File),
                    fun() -> eof end
            end;
        _ ->
            %% Shouldn't get here. If we're here, then the Offset
            %% values are broken in some way.
            file:close(File),
            throw({iterate_term, offset_fail})
    end;
iterate_by_term(File, _, [], _) ->
    file:close(File),
    fun() -> eof end.
    
iterate_by_term_values(File, TransformFun, WhenDoneFun) ->
    %% Read the next value, expose as iterator.
    case read_seg_entry(File) of
        {values, Results} ->
            iterate_by_term_values_1(Results, File, TransformFun, WhenDoneFun);
        Other ->
            WhenDoneFun(Other)
    end.
iterate_by_term_values_1([Result|Results], File, TransformFun, WhenDoneFun) ->
    {TransformFun(Result), fun() -> iterate_by_term_values_1(Results, File, TransformFun, WhenDoneFun) end};
iterate_by_term_values_1([], File, TransformFun, WhenDoneFun) ->
    iterate_by_term_values(File, TransformFun, WhenDoneFun).

%% iterators/5 - Return a list of iterators for all the terms in a
%% given range.
iterators(Index, Field, StartTerm, EndTerm, Size, Segment) ->
    %% Find the Key containing the offset information we need.
    StartKey = {Index, Field, StartTerm},
    EndKey = {Index, Field, EndTerm},
    case get_offset_entry(StartKey, Segment) of
        {OffsetEntryKey, {BlockStart, _, _, _}} ->
            {ok, ReadAheadSize} = application:get_env(merge_index, segment_query_read_ahead_size),
            {ok, FH} = file:open(data_file(Segment), [read, raw, binary, {read_ahead, ReadAheadSize}]),
            file:position(FH, BlockStart),
            iterate_range_by_term(FH, OffsetEntryKey, StartKey, EndKey, Size);
        undefined ->
            [fun() -> eof end]
    end.

%% iterate_range_by_term/5 - Generate a list of iterators matching the
%% provided range. Keep everything in memory for now. Returns the list
%% of iterators. TODO - In the future, once we've amassed enough
%% iterators, write the data out to a separate temporary file.
iterate_range_by_term(File, BaseKey, StartKey, EndKey, Size) ->
    iterate_range_by_term_1(File, BaseKey, StartKey, EndKey, Size, false, [], []).
iterate_range_by_term_1(File, BaseKey, StartKey, EndKey, Size, IterateOverValues, ResultsAcc, IteratorsAcc) ->
    case read_seg_entry(File) of
        {key, ShrunkenKey} ->
            %% Expand the possibly shrunken key...
            CurrKey = expand_key(BaseKey, ShrunkenKey),

            %% If the key is smaller than the one we need, keep
            %% jumping. If it's in the range we need, then iterate
            %% values. Otherwise, it's too big, so close the file and
            %% return.
            if 
                CurrKey < StartKey ->
                    iterate_range_by_term_1(File, CurrKey, StartKey, EndKey, Size, false, [], IteratorsAcc);
                CurrKey =< EndKey ->
                    NewIteratorsAcc = possibly_add_iterator(ResultsAcc, IteratorsAcc),
                    case Size == 'all' orelse size(element(3, CurrKey)) == Size of
                        true ->
                            iterate_range_by_term_1(File, CurrKey, StartKey, EndKey, Size, true, [], NewIteratorsAcc);
                        false ->
                            iterate_range_by_term_1(File, CurrKey, StartKey, EndKey, Size, false, [], NewIteratorsAcc)
                    end;
                CurrKey > EndKey ->
                    file:close(File),
                    possibly_add_iterator(ResultsAcc, IteratorsAcc)
            end;
        {values, Results} when IterateOverValues ->
            iterate_range_by_term_1(File, BaseKey, StartKey, EndKey, Size, true, [Results|ResultsAcc], IteratorsAcc);
        {values, _Results} when not IterateOverValues ->
            iterate_range_by_term_1(File, BaseKey, StartKey, EndKey, Size, false, [], IteratorsAcc);
        eof ->
            %% Shouldn't get here. If we're here, then the Offset
            %% values are broken in some way.
            file:close(File),
            possibly_add_iterator(ResultsAcc, IteratorsAcc)
    end.

possibly_add_iterator([], IteratorsAcc) ->
    IteratorsAcc;
possibly_add_iterator(Results, IteratorsAcc) ->
    Results1 = lists:flatten(lists:reverse(Results)),
    Iterator = fun() -> iterate_list(Results1) end,
    [Iterator|IteratorsAcc].
    
%% Turn a list into an iterator.
iterate_list([]) ->
    eof;
iterate_list([H|T]) ->
    {H, fun() -> iterate_list(T) end}.

%% PRIVATE FUNCTIONS

%% Given a key, look up the entry in the offsets table and return
%% {OffsetKey, StartPos, Offsets, Bloom} or 'undefined'.
get_offset_entry(Key, Segment) ->
    case ets:lookup(Segment#segment.offsets_table, Key) of
        [] ->
            case ets:next(Segment#segment.offsets_table, Key) of
                '$end_of_table' -> 
                    undefined;
                OffsetKey ->    
                    %% Look up the offset information.
                    [{OffsetKey, Value}] = ets:lookup(Segment#segment.offsets_table, OffsetKey),
                    {OffsetKey, binary_to_term(Value)}
            end;
        [{OffsetKey, Value}] ->
            {OffsetKey, binary_to_term(Value)}
    end.


%% Read the offsets file from disk. If it's not found, then recreate
%% it from the data file. Return the offsets tree.
read_offsets(Root) ->
    case ets:file2tab(offsets_file(Root)) of
        {ok, OffsetsTable} ->
            OffsetsTable;
        {error, Reason} ->
            %% TODO - File doesn't exist -- Rebuild it.
            throw({?MODULE, {offsets_file_error, Reason}})
    end.


read_seg_entry(FH) ->
    case file:read(FH, 1) of
        {ok, <<0:1/integer, Size1:7/bitstring>>} ->
            {ok, <<Size2:24/bitstring>>} = file:read(FH, 3),
            <<TotalSize:31/unsigned-integer>> = <<Size1:7/bitstring, Size2:24/bitstring>>,
            {ok, B} = file:read(FH, TotalSize),
            {values, binary_to_term(B)};
        {ok, <<1:1/integer, Size1:7/bitstring>>} ->
            {ok, <<Size2:8/bitstring>>} = file:read(FH, 1),
            <<TotalSize:15/unsigned-integer>> = <<Size1:7/bitstring, Size2:8/bitstring>>,
            {ok, B} = file:read(FH, TotalSize),
            {key, binary_to_term(B)};
        eof ->
            eof
    end.

data_file(Segment) when is_record(Segment, segment) ->
    data_file(Segment#segment.root);
data_file(Root) ->
    Root ++ ".data".

offsets_file(Segment) when is_record(Segment, segment) ->
    offsets_file(Segment#segment.root);
offsets_file(Root) ->
    Root ++ ".offsets".

%% expand_key/2 - Given a BaseKey and a shrunken Key, return
%% the actual key by re-adding the field and term if
%% encessary. Clauses ordered by most common first.
expand_key({Index, Field, _}, Term) when not is_tuple(Term) ->
    {Index, Field, Term};
expand_key({Index, _, _}, {Field, Term}) ->
    {Index, Field, Term};
expand_key(_, {Index, Field, Term}) ->
    {Index, Field, Term}.


%% %% ===================================================================
%% %% EUnit tests
%% %% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POW_2(N), trunc(math:pow(2, N))).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

g_i() ->
    non_empty(binary()).

g_f() ->
    non_empty(binary()).

g_t() ->
    non_empty(binary()).

g_ift() ->
    {g_i(), g_f(), g_t()}.

g_ift_range(IFTs) ->
    ?SUCHTHAT({{I1, F1, _T1}=Start, {I2, F2, _T2}=End},
              {oneof(IFTs), oneof(IFTs)}, (End >= Start) andalso (I1 =:= I2) andalso (F1 =:= F2)).

g_value() ->
    non_empty(binary()).

g_props() ->
    list({oneof([word_pos, offset]), choose(0, ?POW_2(31))}).

g_tstamp() ->
    choose(0, ?POW_2(31)).

get_count(Entries) ->
    fun ({{I1, F1, T1}, _, _, _}) ->
            Fun = fun({I2, F2, T2, _, _, _}) ->
                          (I1 =:= I2) and (F1 =:= F2) and (T1 =:= T2)
                  end,
            length(lists:filter(Fun, Entries))
    end.

use_info(Segment) ->
    fun ({{I, F, T}, _, _, _}) ->
            mi_segment:info(I, F, T, Segment)
    end.

fold_iterator(Itr, Fn, Acc0) ->
    fold_iterator_inner(Itr(), Fn, Acc0).

fold_iterator_inner(eof, _Fn, Acc) ->
    lists:reverse(Acc);
fold_iterator_inner({Term, NextItr}, Fn, Acc0) ->
    Acc = Fn(Term, Acc0),
    fold_iterator_inner(NextItr(), Fn, Acc).

fold_iterators([], _Fun, Acc) ->
    lists:reverse(Acc);
fold_iterators([Itr|Itrs], Fun, Acc0) ->
    ?debugFmt("Itr: ~p~n", [Itr]),
    Acc = fold_iterator(Itr, Fun, Acc0),
    fold_iterators(Itrs, Fun, Acc).

prop_basic_test(Root) ->
    ?FORALL(Entries, list({{g_i(), g_f(), g_t()}, g_value(), g_props(), g_tstamp()}),
            begin
                [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],

                F = fun({{Index, Field, Term}, Value, Props, Tstamp}, Acc) ->
                            Key = {Index, Field, Term, Value},
                            case orddict:find(Key, Acc) of
                                {ok, {_, ExistingTstamp}} when Tstamp >= ExistingTstamp ->
                                    orddict:store(Key, {Props, Tstamp}, Acc);
                                error ->
                                    orddict:store(Key, {Props, Tstamp}, Acc);
                                _ ->
                                    Acc
                            end
                    end,
                L1 = [{Index, Field, Term, Value, Props, Tstamp} ||
                         {{Index, Field, Term, Value}, {Props, Tstamp}}
                             <- lists:foldl(F, [], Entries)],

                Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),
                mi_segment:from_buffer(Buffer, open_write(Root ++ "_segment")),
                Segment = mi_segment:open_read(Root ++ "_segment"),

                %% Fold over the entire segment
                SL = fold_iterator(iterator(Segment),
                                   fun(Item, Acc0) -> [Item | Acc0] end, []),

                Three = lists:sublist(Entries, 3),
                C1 = lists:map(get_count(L1), Three),
                C2 = lists:map(use_info(Segment), Three),

                mi_buffer:delete(Buffer),
                mi_segment:delete(Segment),
                conjunction([{entires, equals(L1, SL)},
                             {info, equals(true, C2 >= C1)}])
            end).

prop_iter_range_test(Root) ->
    ?LET({I, F}, {g_i(), g_f()},
         ?LET(IFTs, non_empty(list(frequency([{10, {I, F, g_t()}}, {1, g_ift()}]))),
              ?FORALL({Entries, Range},
                      {list({oneof(IFTs), g_value(), g_props(), g_tstamp()}), g_ift_range(IFTs)},
                      begin check_range(Root, Entries, Range) end))).

check_range(Root, Entries, Range) ->
    [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],
    Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),
    mi_segment:from_buffer(Buffer, mi_segment:open_write(Root ++ "_segment")),
    Segment = mi_segment:open_read(Root ++ "_segment"),

    {Start, End} = Range,
    {Index, Field, StartTerm} = Start,
    {Index, Field, EndTerm} = End,
    Itrs = mi_segment:iterators(Index, Field, StartTerm, EndTerm, all, Segment),
    L1 = fold_iterators(Itrs, fun(Item, Acc0) -> [Item | Acc0] end, []),

    L2 = [{V, K, P}
          || {Ii, Ff, Tt, V, K, P} <- fold_iterator(mi_segment:iterator(Segment),
                                                    fun(I,A) -> [I|A] end, []),
             {Ii, Ff, Tt} >= Start, {Ii, Ff, Tt} =< End],
    mi_buffer:delete(Buffer),
    mi_segment:delete(Segment),
    equals(lists:sort(L1), lists:sort(L2)).

prop_basic_test_() ->
    test_spec("/tmp/test/mi_segment_basic", fun prop_basic_test/1).

prop_iter_range_test_() ->
    test_spec("/tmp/test/mi_segment_iter_range", fun prop_iter_range_test/1).

test_spec(Root, PropertyFn) ->
    {timeout, 60, fun() ->
                          application:load(merge_index),
                          os:cmd(?FMT("rm -rf ~s; mkdir -p ~s", [Root, Root])),
                          ?assert(eqc:quickcheck(eqc:numtests(250, ?QC_OUT(PropertyFn(Root ++ "/t1")))))
                  end}.

-endif. % EQC
-endif.
