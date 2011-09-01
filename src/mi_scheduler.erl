%% -------------------------------------------------------------------
%%
%% mi_scheduler: makes segment compaction single threaded.
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

-module(mi_scheduler).

%% API
-export([
    start_link/0,
    start/0,
    schedule_compaction/1
]).
%% Private export
-export([worker_loop/1]).

-include("merge_index.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { queue,
                 worker }).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

schedule_compaction(Pid) ->
    gen_server:call(?MODULE, {schedule_compaction, Pid}, infinity).
    

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits of the actual worker process
    process_flag(trap_exit, true),

    %% Use a dedicated worker sub-process to do the actual merging. The
    %% process may ignore messages for a long while during the compaction
    %% and we want to ensure that our message queue doesn't fill up with
    %% a bunch of dup requests for the same directory.
    Self = self(),
    WorkerPid = spawn_link(fun() -> worker_loop(Self) end),
    {ok, #state{ queue = queue:new(),
                 worker = WorkerPid }}.

handle_call({schedule_compaction, Pid}, _From, #state { queue = Q } = State) ->
    case queue:member(Pid, Q) of
        true ->
            {reply, already_queued, State};
        false ->
            NewState = State#state { queue = queue:in(Pid, Q) },
            {reply, ok, NewState}
    end;

handle_call(Event, _From, State) ->
    lager:error("unhandled_call ~p", [Event]),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    lager:error("unhandled_cast ~p", [Msg]),
    {noreply, State}.

handle_info({worker_ready, WorkerPid}, #state { queue = Q } = State) ->
    case queue:out(Q) of
        {empty, Q} ->
            {noreply, State};
        {{value, Pid}, NewQ} ->
            WorkerPid ! {compaction, Pid},
            NewState = State#state { queue=NewQ },
            {noreply, NewState}
    end;
handle_info({'EXIT', WorkerPid, Reason}, #state { worker = WorkerPid } = State) ->
    lager:error("Compaction worker ~p exited: ~p", [WorkerPid, Reason]),
    %% Start a new worker.
    Self=self(),
    NewWorkerPid = spawn_link(fun() -> worker_loop(Self) end),
    NewState = State#state { worker=NewWorkerPid },
    {noreply, NewState};

handle_info(Info, State) ->
    lager:error("unhandled_info ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal worker
%% ====================================================================

worker_loop(Parent) ->
    Parent ! {worker_ready, self()},
    receive
        {compaction, Pid} ->
            Start = now(),
            Result = merge_index:compact(Pid),
            ElapsedSecs = timer:now_diff(now(), Start) / 1000000,
            case Result of
                {ok, OldSegments, OldBytes} ->
                    case ElapsedSecs > 1 of
                        true ->
                            lager:info(
                              "Pid ~p compacted ~p segments for ~p bytes in ~p seconds, ~.2f MB/sec",
                              [Pid, OldSegments, OldBytes, ElapsedSecs, OldBytes/ElapsedSecs/(1024*1024)]);
                        false ->
                            ok
                    end;

                {Error, Reason} when Error == error; Error == 'EXIT' ->
                    lager:error("Failed to compact ~p: ~p", [Pid, Reason])
            end,
            ?MODULE:worker_loop(Parent);
        _ ->
            %% ignore unknown messages
            ?MODULE:worker_loop(Parent)
    after 1000 ->
            ?MODULE:worker_loop(Parent)
    end.

