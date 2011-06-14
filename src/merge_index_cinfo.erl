%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

-module(merge_index_cinfo).
-export([
         cluster_info_init/0, 
         cluster_info_generator_funs/0
        ]).

%% @spec () -> term()
%% @doc Required callback function for cluster_info: initialization.
%%
%% This function doesn't have to do anything.

cluster_info_init() ->
    ok.

%% @spec () -> list({string(), fun()})
%% @doc Required callback function for cluster_info: return list of
%%      {NameForReport, FunOfArity_1} tuples to generate ASCII/UTF-8
%%      formatted reports.

cluster_info_generator_funs() ->
    [
     {"Merge Index Summary", fun summary/1}
    ].

summary(CPid) ->
    {BufferCount, BufferMemory} = ets_table_info(buffer),
    {SegmentCount, SegmentMemory} = ets_table_info(segment_offsets),

    [cluster_info:format(CPid, " ~s: ~p\n", [X, Y]) 
     || {X, Y} <- [
                   {buffer_count, BufferCount},
                   {buffer_memory, BufferMemory},
                   {segment_count, SegmentCount},
                   {segment_offsets_memory, SegmentMemory}]].

ets_table_info(TableName) ->
    %% Get a list of the tables...
    F = fun(X) -> ets:info(X, name) == TableName end,
    Tables = lists:filter(F, ets:all()),
    Count = length(Tables),

    %% Count up the memory used...
    WordSize = erlang:system_info(wordsize),
    Memory = lists:sum([ets:info(X, memory) || X <- Tables]) * WordSize,

    %% Return...
    {Count, Memory}.
