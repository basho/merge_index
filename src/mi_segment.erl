%% -------------------------------------------------------------------
%%
%% mi_segment: ordered index stored on disk with
%%             offset information stored in ETS.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
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


%% MIN_VALUE is used during range searches. Numbers sort the smallest
%% in Erlang (http://www.erlang.org/doc/reference_manual/expressions.html),
%% so this is just a very small number.
-define(MIN_VALUE, -9223372036854775808). %% -1 * (2^63)).

exists(Root) ->
    filelib:is_file(data_file(Root)).

%% Create and return a new segment structure.
open_read(Root) ->
    DataFileExists = filelib:is_file(data_file(Root)),
    case DataFileExists of
        true  ->
            {ok, FileInfo} = file:read_file_info(data_file(Root)),
            OffsetsTable = read_offsets(Root),
            lager:debug("opened segment '~s' for read", [Root]),
            #segment {
                       root=Root,
                       offsets_table=OffsetsTable,
                       size = FileInfo#file_info.size
                     };
        false ->
            throw({?MODULE, missing__file, Root})
    end.

open_write(Root) ->
    DataFileExists = filelib:is_file(data_file(Root)),
    OffsetsFileExists = filelib:is_file(offsets_file(Root)),
    case DataFileExists orelse OffsetsFileExists of
        true  ->
            throw({?MODULE, segment_already_exists, Root});
        false ->
            %% TODO: Do we really need to go through the trouble of writing empty files here?
            file:write_file(data_file(Root), <<"">>),
            file:write_file(offsets_file(Root), <<"">>),
            lager:debug("opened segment '~s' for write", [Root]),
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
            File = data_file(Segment),
            {ok, Bytes} = file:read_file(File),
            fun() -> iterate_all_bytes(undefined, Bytes, File, 0) end;
        false ->
            %% Open a filehandle to the start of the segment.
            {ok, ReadAheadSize} = application:get_env(merge_index, segment_compact_read_ahead_size),
            fun() ->
                    Opts = [read, raw, binary, {read_ahead, ReadAheadSize}],
                    File = data_file(Segment),
                    {ok, FH} = file:open(File, Opts),
                    iterate_all_filehandle({File, FH}, undefined, undefined)
            end
    end.

-define(KEY, <<1:1/integer,
               Size:15/unsigned-integer,
               Bytes:Size/binary,
               Rest/binary>>).
-define(VALUES, <<0:1/integer,
                  Size:31/unsigned-integer,
                  Bytes:Size/binary,
                  Rest/binary>>).

%% @private Create an iterator over a binary which represents the
%% entire segment.
iterate_all_bytes(LastKey, ?KEY, File, Offset) ->
    case try_b2t(Bytes) of
        {ok, MaybePartialKey} ->
            try
                Key = expand_key(LastKey, MaybePartialKey),
                iterate_all_bytes(Key, Rest, File, Offset + Size + 2)
            catch error:function_clause ->
                    StashFile = stash_corrupt(File, Bytes),
                    lager:warning("Corrupted posting key detected in ~s starting"
                                  " at offset ~w with size ~w, stashed in ~s",
                                  [File, Offset, Size + 2, StashFile]),
                    iterate_all_bytes(LastKey, Rest, File, Offset + Size + 2)
            end;
        corrupt ->
            StashFile = stash_corrupt(File, Bytes),
            lager:warning("Corrupted posting key detected in ~s starting"
                          " at offset ~w with size ~w, stashed in ~s",
                          [File, Offset, Size + 2, StashFile]),
            iterate_all_bytes(LastKey, Rest, File, Offset + Size + 2)
    end;

iterate_all_bytes(Key, ?VALUES, File, Offset) when Key /= undefined ->
    case try_b2t(Bytes) of
        {ok, Values} ->
            try
                iterate_all_bytes_1(Key, Values, Rest, File, Offset + Size + 4)
            catch error:function_clause ->
                    %% This happens when Values is not a list, use
                    %% try/catch because this should be exceptional
                    %% case.
                    StashFile = stash_corrupt(File, Bytes),
                    lager:warning("Corrupted posting values detected in ~s for key ~p"
                                  "starting at offset ~w with size ~w, stashed in ~s",
                                  [File, Key, Offset, Size + 4, StashFile]),
                    iterate_all_bytes(Key, Rest, File, Offset + Size + 4);
                  error:badmatch ->
                    %% This happens if the `Result' in
                    %% `iterate_all_bytes_1' is corrupted, e.g. bytes
                    %% corrupted in such a way that tuple arity is not
                    %% 3.  This could be done in `iterate_all_bytes_1'
                    %% to be more fine grained but it makes the
                    %% logging more complex to calculate correct
                    %% offset.
                    StashFile = stash_corrupt(File, Bytes),
                    lager:warning("Corrupted posting values detected in ~s for key ~p"
                                  "starting at offset ~w with size ~w, stashed in ~s",
                                  [File, Key, Offset, Size + 4, StashFile]),
                    iterate_all_bytes(Key, Rest, File, Offset + Size + 4)
            end;
        corrupt ->
            StashFile = stash_corrupt(File, Bytes),
            lager:warning("Corrupted posting values detected in ~s for key ~p"
                          "starting at offset ~w with size ~w, stashed in ~s",
                          [File, Key, Offset, Size + 4, StashFile]),
            iterate_all_bytes(Key, Rest, File, Offset + Size + 4)
    end;

iterate_all_bytes(_, <<>>, _, _) ->
    eof;

iterate_all_bytes(LastKey, Bytes, File, Offset) ->
    StashFile = stash_corrupt(File, Bytes),
    lager:warning("Corrupted data detected in ~s with last key of ~p"
                  "starting at offset ~w with size ~w, stashed in ~s,"
                  " ignoring rest of file",
                  [File, LastKey, Offset, size(Bytes) + 4, StashFile]),
    eof.

iterate_all_bytes_1(Key, [Result|Results], Rest, File, Offset) ->
    {I,F,T} = Key,
    {V,K,P} = Result,
    {{I,F,T,V,K,P},
     fun() -> iterate_all_bytes_1(Key, Results, Rest, File, Offset) end};

iterate_all_bytes_1(Key, [], Rest, File, Offset) ->
    iterate_all_bytes(Key, Rest, File, Offset).

%% @private Create an iterator over a filehandle starting at position
%% 0 of the segment.
iterate_all_filehandle({File, FH}, BaseKey, {key, ShrunkenKey, _}) ->
    CurrKey = expand_key(BaseKey, ShrunkenKey),
    {I,F,T} = CurrKey,
    Transform = fun({V,K,P}) -> {I,F,T,V,K,P} end,
    WhenDone = fun(NextEntry) -> iterate_all_filehandle({File, FH}, CurrKey, NextEntry) end,
    iterate_by_term_values({File, FH}, Transform, WhenDone);
iterate_all_filehandle({File, FH}, BaseKey, undefined) ->
    iterate_all_filehandle({File, FH}, BaseKey, read_seg_entry({File, FH}));
iterate_all_filehandle({_File, FH}, _, eof) ->
    file:close(FH),
    eof;
iterate_all_filehandle({File, FH}, BaseKey, {values, Values, {Offset, Size}}) ->
    StashFile = stash_corrupt(File, term_to_binary(Values)),
    lager:warning("Corrupted posting key detected in ~s at offset ~w of"
                  " size ~w, stashed in ~s",
                  [File, Offset, Size, StashFile]),
    %% Expected a key entry on last read, thus ignore the next entry
    %% would should be a value.
    case read_seg_entry({File,FH}) of
        eof ->
            eof;
        _ ->
            iterate_all_filehandle({File,FH}, BaseKey, read_seg_entry({File,FH}))
    end.

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
            File = data_file(Segment),
            {ok, FH} = file:open(File, [read, raw, binary, {read_ahead, ReadAheadSize}]),
            file:position(FH, FileOffset),
            iterate_by_term({File, FH}, BaseKey, [Match|Rest], Key)
    end;
iterate_by_keyinfo(_, _, _, _, _, [], _) ->
    fun() -> eof end.

%% Iterate over the segment file until we find the start of the values
%% section we want.
iterate_by_term({File, FH}, BaseKey,
                [{_, _, _, ValuesSize, _}|KeyInfoList], Key) ->
    %% Read the next entry in the segment file.  Value should be a
    %% key, otherwise error.
    case read_seg_entry({File, FH}) of
        {key, ShrunkenKey, _} ->
            CurrKey = expand_key(BaseKey, ShrunkenKey),
            %% If the key is smaller than the one we need, keep
            %% jumping. If it's the one we need, then iterate
            %% values. Otherwise, it's too big, so close the file and
            %% return.
            if
                CurrKey < Key ->
                    file:read(FH, ValuesSize),
                    iterate_by_term({File, FH}, CurrKey, KeyInfoList, Key);
                CurrKey == Key ->
                    Transform = fun(X) -> X end,
                    WhenDone = fun(_) -> file:close(FH), eof end,
                    fun() -> iterate_by_term_values({File, FH}, Transform, WhenDone) end;
                CurrKey > Key ->
                    file:close(FH),
                    fun() -> eof end
            end;
        _ ->
            %% Shouldn't get here. If we're here, then the Offset
            %% values are broken in some way.
            %%
            %% TODO: corruption resilient
            file:close(FH),
            throw({iterate_term, offset_fail})
    end;
iterate_by_term({_,FH}, _, [], _) ->
    file:close(FH),
    fun() -> eof end.

iterate_by_term_values({File, FH}, TransformFun, WhenDoneFun) ->
    %% Read the next value, expose as iterator.
    case read_seg_entry({File, FH}) of
        {values, Results, _} ->
            iterate_by_term_values_1(Results, {File, FH}, TransformFun, WhenDoneFun);
        Other ->
            WhenDoneFun(Other)
    end.

iterate_by_term_values_1([Result|Results], {File, FH}, TransformFun, WhenDoneFun) ->
    {TransformFun(Result),
     fun() ->
             iterate_by_term_values_1(Results, {File, FH},
                                      TransformFun, WhenDoneFun)
     end};
iterate_by_term_values_1([], {File, FH}, TransformFun, WhenDoneFun) ->
    iterate_by_term_values({File, FH}, TransformFun, WhenDoneFun).

%% iterators/5 - Return a list of iterators for all the terms in a
%% given range.
iterators(Index, Field, StartTerm, EndTerm, Size, Segment) ->
    %% If the user has passed in 'undefined' for the StartTerm, then
    %% replace with ?MIN_VALUE, signaling that we don't want a lower
    %% bound. An EndTerm of 'undefined' is handled within iterate_range_by_term/7.
    StartTerm1 = case StartTerm == undefined of
                     true  -> ?MIN_VALUE;
                     false -> StartTerm
                 end,

    %% Find the Key containing the offset information we need
    StartKey = {Index, Field, StartTerm1},
    case get_offset_entry(StartKey, Segment) of
        {OffsetEntryKey, {BlockStart, _, _, _}} ->
            {ok, ReadAheadSize} = application:get_env(merge_index, segment_query_read_ahead_size),
            File = data_file(Segment),
            {ok, FH} = file:open(File, [read, raw, binary, {read_ahead, ReadAheadSize}]),
            file:position(FH, BlockStart),
            iterate_range_by_term({File, FH}, OffsetEntryKey, Index, Field,
                                  StartTerm1, EndTerm, Size);
        undefined ->
            [fun() -> eof end]
    end.

%% iterate_range_by_term/5 - Generate a list of iterators matching the
%% provided range. Keep everything in memory for now. Returns the list
%% of iterators. TODO - In the future, once we've amassed enough
%% iterators, write the data out to a separate temporary file.
iterate_range_by_term({File, FH}, BaseKey, Index, Field, StartTerm, EndTerm, Size) ->
    iterate_range_by_term_1({File, FH}, BaseKey, Index, Field, StartTerm, EndTerm,
                            Size, false, [], []).

iterate_range_by_term_1({File, FH}, BaseKey, Index, Field, StartTerm, EndTerm,
                        Size, IterateOverValues, ResultsAcc, IteratorsAcc) ->
    case read_seg_entry({File, FH}) of
        {key, ShrunkenKey, _} ->
            CurrKey = {I, F, T} = expand_key(BaseKey, ShrunkenKey),

            %% If the key is smaller than the one we need, keep
            %% jumping. If it's in the range we need, then iterate
            %% values. Otherwise, it's too big, so close the file and
            %% return.
            if
                CurrKey < {Index, Field, StartTerm} ->
                    iterate_range_by_term_1({File, FH}, CurrKey, Index, Field,
                                            StartTerm, EndTerm, Size,
                                            false, [], IteratorsAcc);
                I == Index andalso F == Field
                andalso (EndTerm == undefined orelse T =< EndTerm) ->
                    NewIteratorsAcc = possibly_add_iterator(BaseKey,
                                                            ResultsAcc,
                                                            IteratorsAcc),
                    case Size == 'all' orelse size(StartTerm) == Size of
                        true ->
                            iterate_range_by_term_1({File, FH}, CurrKey, Index,
                                                    Field, StartTerm,
                                                    EndTerm, Size, true,
                                                    [], NewIteratorsAcc);
                        false ->
                            iterate_range_by_term_1({File, FH}, CurrKey, Index,
                                                    Field, StartTerm, EndTerm,
                                                    Size, false, [],
                                                    NewIteratorsAcc)
                    end;
                true ->
                    file:close(FH),
                    possibly_add_iterator(BaseKey, ResultsAcc, IteratorsAcc)
            end;
        {values, _Results, _} when not IterateOverValues ->
            iterate_range_by_term_1({File, FH}, BaseKey, Index, Field, StartTerm,
                                    EndTerm, Size, false, [], IteratorsAcc);
        {values, Results, _} when IterateOverValues ->
            iterate_range_by_term_1({File, FH}, BaseKey, Index, Field, StartTerm,
                                    EndTerm, Size, true, [Results|ResultsAcc],
                                    IteratorsAcc);
        eof ->
            %% Shouldn't get here. If we're here, then the Offset
            %% values are broken in some way.
            file:close(FH),
            possibly_add_iterator(BaseKey, ResultsAcc, IteratorsAcc)
    end.

possibly_add_iterator(_, [], IteratorsAcc) ->
    IteratorsAcc;
possibly_add_iterator({_, Field, Term}, Results, IteratorsAcc) ->
    Results1 = lists:flatten(lists:reverse(Results)),
    Iterator = fun() -> iterate_list(Field, Term, Results1) end,
    [Iterator|IteratorsAcc].

%% Turn a list into an iterator.
iterate_list(_, _, []) ->
    eof;
iterate_list(Field, Term, [H|T]) ->
    {H, fun() -> iterate_list(Field, Term, T) end}.

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


-spec read_seg_entry({term(),term()}) ->
                            {key, term(), {integer(), integer()}} |
                            {values, term(), {integer(), integer()}} |
                            eof.
read_seg_entry({File, FH}) ->
    {ok, Offset} = file:position(FH, cur),
    case file:read(FH, 1) of
        {ok, <<0:1/integer, Size1:7/bitstring>>} ->
            {ok, <<Size2:24/bitstring>>} = file:read(FH, 3),
            <<TotalSize:31/unsigned-integer>> = <<Size1:7/bitstring, Size2:24/bitstring>>,
            {ok, B} = file:read(FH, TotalSize),
            case try_b2t(B) of
                {ok, Values} ->
                    {values, Values, {Offset, TotalSize}};
                corrupt ->
                    StashFile = stash_corrupt(File, B),
                    lager:warning("Corrupted posting values detected in ~s"
                                  " starting at offset ~w with size ~w,"
                                  " stashed in ~s",
                                  [File, Offset, TotalSize, StashFile]),
                    read_seg_entry({File, FH})
            end;
        {ok, <<1:1/integer, Size1:7/bitstring>>} ->
            {ok, <<Size2:8/bitstring>>} = file:read(FH, 1),
            <<TotalSize:15/unsigned-integer>> = <<Size1:7/bitstring, Size2:8/bitstring>>,
            {ok, B} = file:read(FH, TotalSize),

            case try_b2t(B) of
                {ok, Key} ->
                    {key, Key, {Offset, TotalSize}};
                corrupt ->
                    StashFile = stash_corrupt(File, B),
                    lager:warning("Corrupted posting key detected in ~s"
                                  " starting at offset ~w with size ~w,"
                                  " stashed in ~s",
                                  [File, Offset, TotalSize, StashFile]),
                    read_seg_entry({File, FH})
            end;
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

try_b2t(Binary) ->
    try
        {ok, binary_to_term(Binary)}
    catch
        error:badarg ->
            corrupt
    end.

stash_corrupt(Filename, CorruptedBinary) ->
    StashFile = Filename ++ "-" ++ integer_to_list(element(3,now())),
    file:write_file(StashFile, CorruptedBinary),
    StashFile.
