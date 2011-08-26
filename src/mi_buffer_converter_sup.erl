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
-module(mi_buffer_converter_sup).
-behaviour(supervisor).

-export([init/1,
         start_link/0,
         start_child/3]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Server, Root, Buffer) ->
    supervisor:start_child(mi_buffer_converter_sup, [Server, Root, Buffer]).

init([]) ->
    Spec = {undefined,
            {mi_buffer_converter, start_link, []},
            temporary, 1000, worker, [mi_buffer_converter]},
    {ok, {{simple_one_for_one, 10, 1}, [Spec]}}.
