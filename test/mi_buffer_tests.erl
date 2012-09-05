%% -------------------------------------------------------------------
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

-module(mi_buffer_tests).
-import(common, [entries/0, g_i/0, g_f/0, g_t/0, g_ift/0, g_ift_range/1, g_value/0,
                 g_props/0, g_tstamp/0, fold_iterator/3, fold_iterators/3,
                 unique_latest/2, test_spec/2, test_spec/3]).

-ifdef(EQC).

-compile(export_all).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("common.hrl").

prop_basic_test(Root) ->
    ?FORALL(Entries, list({g_ift(), g_value(), g_tstamp(), g_props()}),
            check_entries(Root, Entries)).

prop_dups_test(Root) ->
    ?FORALL(Entries, list(default({{<<0>>,<<0>>,<<0>>},<<0>>,[],0},
                                  {g_ift(), g_value(), g_tstamp(), g_props()})),
            check_entries(Root, Entries)).

check_entries(Root, Entries) ->
    BufName = Root ++ "_buffer",
    Buffer = mi_buffer:write(Entries, mi_buffer:new(BufName)),

    L1 = [{I, F, T, Value, Tstamp, Props}
          || {{I, F, T}, Value, Tstamp, Props} <- Entries],
    ES = length(L1),

    L2 = fold_iterator(mi_buffer:iterator(Buffer),
                       fun(Item, Acc0) -> [Item | Acc0] end, []),
    AS = mi_buffer:size(Buffer),
    Name = mi_buffer:filename(Buffer),

    mi_buffer:delete(Buffer),
    conjunction([{postings, equals(lists:sort(L1), lists:sort(L2))},
                 {size, equals(ES, AS)},
                 {filename, equals(BufName, Name)}]).

prop_iter_range_test(Root) ->
    ?LET({I, F},
         {g_i(), g_f()},
    ?LET(IFTs,
         non_empty(list(frequency([{10, {I, F, g_t()}}, {1, g_ift()}]))),
    ?FORALL({Entries, Range},
            {list({oneof(IFTs), g_value(), g_tstamp(), g_props()}),
             g_ift_range(IFTs)},
            check_range(Root, Entries, Range)))).

check_range(Root, Entries, Range) ->
    Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),

    {Start, End} = Range,
    {Index, Field, StartTerm} = Start,
    {Index, Field, EndTerm} = End,

    L1 = [{V, K, P} || {IFT, V, K, P} <- Entries,
                       IFT >= Start, IFT =< End],

    Itrs = mi_buffer:iterators(Index, Field, StartTerm, EndTerm, all, Buffer),
    L2 = fold_iterators(Itrs, fun(Item, Acc0) -> [Item | Acc0] end, []),

    mi_buffer:delete(Buffer),
    equals(lists:sort(L1), lists:sort(L2)).

prop_info_test(Root) ->
    ?LET(IFT, g_ift(),
    ?LET(IFTs, non_empty(list(frequency([{10, IFT}, {1, g_ift()}]))),
    ?FORALL(Entries,
            list({oneof(IFTs), g_value(), g_tstamp(), g_props()}),
            check_count(Root, Entries, IFT)))).

check_count(Root, Entries, IFT) ->
    Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),

    {Ie, Fe, Te} = IFT,
    L1 = [X || {{I, F, T}, _, _, _} = X <- Entries,
               (I =:= Ie) andalso (F =:= Fe) andalso (T =:= Te)],

    C1 = length(L1),
    C2 = mi_buffer:info(Ie, Fe, Te, Buffer),
    mi_buffer:delete(Buffer),
    equals(C1, C2).

prop_corruption_test(Root) ->
    ?LET(Entries, entries(),
         begin
             BufName = Root ++ "_buffer",
             Buffer = mi_buffer:write(Entries, mi_buffer:new(BufName)),
             Filename = mi_buffer:filename(Buffer),
             Tab = element(4, Buffer),
             ets:delete(Tab),
             mi_buffer:close_filehandle(Buffer),
             {ok, Bin} = file:read_file(Filename),
             Size = size(Bin),
             ?FORALL({Pos,RandomByte}, {choose(1, Size-1), binary(1)},
                     begin
                         BeforePos = Pos - 1,
                         <<Before:BeforePos/binary,_:1/binary,After/binary>> = Bin,
                         Bin2 = <<Before/binary,RandomByte/binary,After/binary>>,
                         ok = file:write_file(Filename, Bin2),
                         _ = mi_buffer:new(BufName)
                     end)
         end).

prop_truncation_test(Root) ->
    ?LET(Entries,
         non_empty(list({g_ift(), g_value(), g_tstamp(), g_props()})),
         begin
             try
                 Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),
                 BufferLen = mi_buffer:filesize(Buffer),
                 ?FORALL(TruncateBy,
                         choose(1, BufferLen-1), check_truncation(Buffer, Entries, TruncateBy))
              after
                 catch file:delete(Root ++ "_buffer")
              end
         end).

check_truncation({buffer, Filename, FileHandle, EtsTable, _OrigSize} = Buffer, Entries, TruncateBy) ->
    %% This is vulnerable to record format changes. mi_buffer should
    %% provide an hrl with the record or its own "close" function.
    {ok, _Offset} = file:position(FileHandle, {eof, TruncateBy}),
    ok = file:truncate(FileHandle),
    mi_buffer:close_filehandle(Buffer),
    ets:delete(EtsTable),
    NewBuffer = mi_buffer:new(Filename),
    {buffer,_,_,NewEts,_} = NewBuffer,
    try
        Stored = ets:tab2list(NewEts),
        equals([], [IFT || IFT <- Stored,
                           not lists:member(IFT, Entries)])
    after
        mi_buffer:delete(NewBuffer)
    end.

prop_basic_test_() ->
    test_spec("/tmp/test/mi_buffer_basic", fun prop_basic_test/1).

prop_dups_test_() ->
    test_spec("/tmp/test/mi_buffer_dups", fun prop_dups_test/1).

prop_iter_range_test_() ->
    test_spec("/tmp/test/mi_buffer_iter", fun prop_iter_range_test/1).

prop_info_test_() ->
    test_spec("/tmp/test/mi_buffer_info", fun prop_info_test/1).

prop_truncation_test_() ->
    test_spec("/tmp/test/mi_buffer_truncation", fun prop_truncation_test/1).

prop_corruption_test_() ->
    test_spec("/tmp/test/mi_buffer_corruption", fun prop_corruption_test/1).

-endif.
