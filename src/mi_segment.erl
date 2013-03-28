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
    staleness/2,
    is_stale/2,
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

-export([
    compact_by_average/1,
    compact_by_average_and_staleness/1,
    compact_all/1
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
    %% Create the file if it doesn't exist...
    DataFileExists = filelib:is_file(data_file(Root)),
    case DataFileExists of
        true  ->
            %% Get the fileinfo...
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

staleness(Segment, TimeScalar) ->
    %% Express staleness by taking the current time - the local time. If it's negative,
    %% just make it zero, because files created in the future are likely very fresh.
    %% Integer division will give us some chunky steps
    SecondsStale = dt2gs(calendar:local_time()) - dt2gs(filelib:last_modified(data_file(Segment#segment.root))),
    case SecondsStale < 0 of
    	false ->
	    	0
    end,
	
    case TimeScalar of
    	second ->
    		SecondsStale;
    	seconds ->
    		SecondsStale;
    	minute ->
    		SecondsStale div 60;
    	minutes ->
    		SecondsStale div 60;
    	hour ->
    		SecondsStale div 3600;
    	hours ->
    		SecondsStale div 3600;
    	day ->
    		SecondsStale div 86400;
    	days ->
    		SecondsStale div 86400;
    	_ ->
    		error(badarg, TimeScalar)
    end.

is_stale(Segment, StalenessThreshold) ->
	IsStale = staleness(Segment, element(2, StalenessThreshold)) >= element(1, StalenessThreshold),
    lager:debug("is_stale(~p,~p) = ~p~n",[Segment, StalenessThreshold, IsStale]),
	IsStale.

dt2gs(DateTime) ->
	%% Just a helper to make the staleness function a little less ugly.
    calendar:datetime_to_gregorian_seconds(DateTime).

delete(Segment) ->
    [ok = file:delete(X) || X <- filelib:wildcard(Segment#segment.root ++ ".*")],
    ets:delete(Segment#segment.offsets_table),
    ok.

%% @doc Given a list of segments calculate a subset of them to merge.
%%
%%   `Segments' - The list of potential segments to merge.
%%
%%   `ToMerge' - The list of segments to merge.
-spec compact_by_average(segments()) -> ToMerge::segments().
compact_by_average(Segments) ->
    lager:debug("compact_by_average(~p)~n",[Segments]),
    %% Take the average of segment sizes, return anything smaller than
    %% the average for merging.
    F1 = fun(X) ->
        Size = mi_segment:filesize(X),
        {Size, X}
    end,
    SortedSizedSegments = lists:sort([F1(X) || X <- Segments]),
    Avg = lists:sum([Size || {Size, _} <- SortedSizedSegments]) div length(Segments) + 1024,
    [Segment || {Size, Segment} <- SortedSizedSegments, Size < Avg].

%% @doc Given a list of segments calculate a subset of them to merge.  This is similar
%%      to the original version, but adds a staleness check into the equation which will
%%      prevent old, large files from not being merged.
%%
%%   `Segments' - The list of potential segments to merge.
%%
%%   `ToMerge' - The list of segments to merge.
-spec compact_by_average_and_staleness(segments()) -> ToMerge::segments().
compact_by_average_and_staleness(Segments) ->
    lager:debug("compact_by_average_and_staleness(~p)~n",[Segments]),
    %% Take the average of segment sizes, return anything smaller than
    %% the average for merging.
    {ok, StalenessThreshold} = application:get_env(merge_index, compact_staleness_threshold),
    F1 = fun(X) ->
        Size = mi_segment:filesize(X),
        IsStale = mi_segment:is_stale(X, StalenessThreshold),
        {Size, IsStale, X}
    end,
    SortedSizedSegments = lists:sort([F1(X) || X <- Segments]),
    Avg = lists:sum([Size || {Size, _} <- SortedSizedSegments]) div length(Segments) + 1024,
	
    %% Lets try to keep the segments fresh by compacting those stale segments
    [Segment || {Size, IsStale, Segment} <- SortedSizedSegments, Size < Avg or IsStale].

%% @doc Given a list of segments return all the things to merge.
%%
%%   `Segments' - The list of potential segments to merge.
%%
%%   `ToMerge' - The list of segments to merge.
-spec compact_all(segments()) -> ToMerge::segments().
compact_all(Segments) ->
    lager:debug("compact_all(~p)~n",[Segments]),
    Segments.

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
            fun() ->
                    Opts = [read, raw, binary, {read_ahead, ReadAheadSize}],
                    {ok, FH} = file:open(data_file(Segment), Opts),
                    iterate_all_filehandle(FH, undefined, undefined)
            end
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
                    Transform = fun(X) -> X end,
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
            {ok, FH} = file:open(data_file(Segment), [read, raw, binary, {read_ahead, ReadAheadSize}]),
            file:position(FH, BlockStart),
            iterate_range_by_term(FH, OffsetEntryKey, Index, Field,
                                  StartTerm1, EndTerm, Size);
        undefined ->
            [fun() -> eof end]
    end.

%% iterate_range_by_term/5 - Generate a list of iterators matching the
%% provided range. Keep everything in memory for now. Returns the list
%% of iterators. TODO - In the future, once we've amassed enough
%% iterators, write the data out to a separate temporary file.
iterate_range_by_term(File, BaseKey, Index, Field, StartTerm, EndTerm, Size) ->
    iterate_range_by_term_1(File, BaseKey, Index, Field, StartTerm, EndTerm,
                            Size, false, [], []).
iterate_range_by_term_1(File, BaseKey, Index, Field, StartTerm, EndTerm, Size,
                        IterateOverValues, ResultsAcc, IteratorsAcc) ->
    case read_seg_entry(File) of
        {key, ShrunkenKey} ->
            %% Expand the possibly shrunken key...
            CurrKey = {I, F, T} = expand_key(BaseKey, ShrunkenKey),

            %% If the key is smaller than the one we need, keep
            %% jumping. If it's in the range we need, then iterate
            %% values. Otherwise, it's too big, so close the file and
            %% return.
            if
                CurrKey < {Index, Field, StartTerm} ->
                    iterate_range_by_term_1(File, CurrKey, Index, Field,
                                            StartTerm, EndTerm, Size,
                                            false, [], IteratorsAcc);
                I == Index andalso F == Field
                andalso (EndTerm == undefined orelse T =< EndTerm) ->
                    NewIteratorsAcc = possibly_add_iterator(BaseKey,
                                                            ResultsAcc,
                                                            IteratorsAcc),
                    case Size == 'all' orelse size(StartTerm) == Size of
                        true ->
                            iterate_range_by_term_1(File, CurrKey, Index,
                                                    Field, StartTerm,
                                                    EndTerm, Size, true,
                                                    [], NewIteratorsAcc);
                        false ->
                            iterate_range_by_term_1(File, CurrKey, Index,
                                                    Field, StartTerm, EndTerm,
                                                    Size, false, [],
                                                    NewIteratorsAcc)
                    end;
                true ->
                    file:close(File),
                    possibly_add_iterator(BaseKey, ResultsAcc, IteratorsAcc)
            end;
        {values, _Results} when not IterateOverValues ->
            iterate_range_by_term_1(File, BaseKey, Index, Field, StartTerm,
                                    EndTerm, Size, false, [], IteratorsAcc);
        {values, Results} when IterateOverValues ->
            iterate_range_by_term_1(File, BaseKey, Index, Field, StartTerm,
                                    EndTerm, Size, true, [Results|ResultsAcc],
                                    IteratorsAcc);
        eof ->
            %% Shouldn't get here. If we're here, then the Offset
            %% values are broken in some way.
            file:close(File),
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
