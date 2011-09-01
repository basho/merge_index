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

-module(merge_index_tests).
-compile(export_all).
-import(common, [g_i/0, g_f/0, g_t/0, g_props/0, g_value/0]).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("common.hrl").

-record(state, {server_pid,
                postings=[]}).

prop_api_test_() ->
    {timeout, 600,
     fun() ->
            ?assert(eqc:quickcheck(eqc:numtests(300,?QC_OUT(prop_api()))))
     end
    }.

prop_api() ->
    application:load(merge_index),
    application:start(sasl),
    application:start(lager),

    %% Comment out following lines to see error reports...otherwise
    %% it's too much noise
    error_logger:delete_report_handler(sasl_report_tty_h),
    lager:set_loglevel(lager_console_backend, critical),

    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
               begin
                   application:stop(merge_index),
                   ok = application:start(merge_index),

                   {H, S, Res} = run_commands(?MODULE, Cmds),

                   Pid = S#state.server_pid,
                   if Pid /= undefined -> merge_index:stop(Pid);
                      true -> ok
                   end,

                   case Res of
                       ok -> ok;
                       _ -> io:format(user,
                                      "QC Commands: ~p~n"
                                      "QC History: ~p~n"
                                      "QC State: ~p~n"
                                      "QC result: ~p~n",
                                      [Cmds, H, S, Res])
                   end,

                   aggregate(command_names(Cmds), Res == ok)
               end)).

%% ====================================================================
%% eqc_statem callbacks
%% ====================================================================

initial_state() ->
    #state{}.

command(#state{server_pid=undefined}) ->
    {call,?MODULE,init,[g_settings()]};
command(S) ->
    P = S#state.server_pid,
    Postings = S#state.postings,
    oneof([{call,?MODULE,index, [P, g_postings()]},
           {call,?MODULE,is_empty, [P]},
           {call,?MODULE,info, [P, g_posting(Postings)]},
           {call,?MODULE,fold, [P, fun fold_fun/7, []]},
           %% TODO generate filter funs for lookup/range
           {call,?MODULE,lookup, [P, g_posting(Postings)]},
           {call,?MODULE,lookup_sync, [P, g_posting(Postings)]},
           %% TODO don't hardcode size to 'all'
           {call,?MODULE,range, [P, g_range_query(Postings), all]},
           {call,?MODULE,range_sync, [P, g_range_query(Postings), all]},
           {call,?MODULE,drop, [P]},
           {call,?MODULE,compact, [P]}]).

next_state(S, Pid, {call,_,init,_}) ->
    S#state{server_pid=Pid};
next_state(S, _Res, {call,_,index,[_,Postings]}) ->
    case Postings of
        [] -> S;
        _ ->
            Postings0 = S#state.postings,
            S#state{postings=Postings0++Postings}
    end;
next_state(S, _Res, {call,_,drop,_}) ->
    S#state{postings=[]};
next_state(S, _Res, {call,_,_,_}) -> S.

precondition(_,_) ->
    true.

postcondition(S, {call,_,is_empty,_}, V) ->
    case S#state.postings of
        [] -> ok == ?assertEqual(true, V);
        _ -> ok == ?assertEqual(false, V)
    end;
postcondition(_, {call,_,index,_}, V) ->
    ok == ?assertEqual(ok, V);
postcondition(#state{postings=Postings}, {call,_,info,[_,{I,F,T,_,_,_}]}, V) ->
    Strip = [{Ii,Ff,Tt,Vv} || {Ii,Ff,Tt,Vv} <- Postings],
    Uniq = ordsets:to_list(ordsets:from_list(Strip)),
    L = [x || {Ii,Ff,Tt,_} <- Uniq,
              (I == Ii) andalso (F == Ff) andalso (T == Tt)],
    {ok, W} = V,

    %% Assert that the weight is _greater than or equal_ b/c the
    %% bloom filter could cause false positives.
    ok == ?assert(W >= length(L));
postcondition(#state{postings=Postings}, {call,_,fold,_}, {ok, V}) ->
    %% NOTE: The order in which fold returns postings is not
    %% deterministic.

    %% Each member of V should be a member of Postings, they aren't
    %% exactly equal b/c some of the dups might have been removed
    %% underneath -- this is confusing behavior if you ask me.
    V2 = lists:sort(V),
    Postings2 = lists:sort(Postings),
    P = fun(E) ->
                lists:member(E, Postings2)
        end,
    ok == ?assert(lists:all(P,V2));

postcondition(#state{postings=Postings},
              {call,_,lookup,[_,{I,F,T,_,_,_}]}, V) ->
    %% TODO This is actually testing the wrong property b/c there is
    %% currently a bug in the range call that causes Props vals to be
    %% lost -- https://issues.basho.com/show_bug.cgi?id=1099
    L1 = lists:sort([{Ii,Ff,Tt,Vv,-1*TS,P} || {Ii,Ff,Tt,Vv,P,TS} <- Postings]),
    L2 = [{Vv,ignore} || {Ii,Ff,Tt,Vv,_,_} <- L1,
                   (I == Ii) andalso (F == Ff) andalso (T == Tt)],
    L3 = lists:foldl(fun unique_vals/2, [], lists:sort(L2)),
    V2 = [{Val,ignore} || {Val,_} <- lists:sort(iterate(V))],
    ok == ?assertEqual(L3, V2);

postcondition(#state{postings=Postings},
              {call,_,lookup_sync,[_,{I,F,T,_,_,_}]}, V) ->
    %% TODO This is actually testing the wrong property b/c there is
    %% currently a bug in the range call that causes Props vals to be
    %% lost -- https://issues.basho.com/show_bug.cgi?id=1099
    L1 = lists:sort([{Ii,Ff,Tt,Vv,-1*TS,P} || {Ii,Ff,Tt,Vv,P,TS} <- Postings]),
    L2 = [{Vv,ignore} || {Ii,Ff,Tt,Vv,_,_} <- L1,
                   (I == Ii) andalso (F == Ff) andalso (T == Tt)],
    L3 = lists:foldl(fun unique_vals/2, [], lists:sort(L2)),
    V2 = [{Val,ignore} || {Val,_} <- lists:sort(V)],
    ok == ?assertEqual(L3, V2);

postcondition(#state{postings=Postings},
              {call,_,range,[_,{I,F,ST,ET},all]}, V) ->
    L1 = lists:sort([{Ii,Ff,Tt,Vv,-1*TS,P} || {Ii,Ff,Tt,Vv,P,TS} <- Postings]),
    L2 = [{Vv,ignore} || {Ii,Ff,Tt,Vv,_,_} <- L1,
                   (I == Ii) andalso (F == Ff)
                             andalso (ST == undefined orelse ST =< Tt)
                             andalso (ET == undefined orelse ET >= Tt)],
    %% TODO This is actually testing the wrong property b/c there is
    %% currently a bug in the range call that causes Props vals to be
    %% lost -- https://issues.basho.com/show_bug.cgi?id=1099 --
    %% uncomment the following line when it's fixed

    L3 = lists:foldl(fun unique_vals/2, [], lists:sort(L2)),
    V2 = [{Val,ignore} || {Val,_} <- lists:sort(iterate(V))],
    ok == ?assertEqual(L3, V2);

postcondition(#state{postings=Postings},
              {call,_,range_sync,[_,{I,F,ST,ET},all]}, V) ->
    L1 = lists:sort([{Ii,Ff,Tt,Vv,-1*TS,P} || {Ii,Ff,Tt,Vv,P,TS} <- Postings]),
    L2 = [{Vv,ignore} || {Ii,Ff,Tt,Vv,_,_} <- L1,
                   (I == Ii) andalso (F == Ff)
                             andalso (ST == undefined orelse ST =< Tt)
                             andalso (ET == undefined orelse ET >= Tt)],
    %% TODO This is actually testing the wrong property b/c there is
    %% currently a bug in the range call that causes Props vals to be
    %% lost -- https://issues.basho.com/show_bug.cgi?id=1099 --
    %% uncomment the following line when it's fixed

    %% L3 = lists:sort((ordsets:from_list(L2)),
    L3 = lists:foldl(fun unique_vals/2, [], lists:sort(L2)),
    V2 = [{Val,ignore} || {Val,_} <- lists:sort(V)],
    ok == ?assertEqual(L3, V2);
postcondition(#state{postings=[]}, {call,_,drop,_}, V) ->
    ok == ?assertEqual(ok, V);
postcondition(_, {call,_,compact,[_]}, V) ->
    {Msg, _SegsCompacted, _BytesCompacted} = V,
    ok == ?assertEqual(ok, Msg);
postcondition(_,_,_) -> true.

%% ====================================================================
%% generators
%% ====================================================================

g_size() ->
    choose(64, 1024).

g_ms() ->
    choose(100, 5000).

g_settings() ->
    [g_size(), g_size(), g_ms(), choose(1,20), g_size(), g_size(),
     g_size(), g_ms(), g_size(), g_size(), choose(1,20), choose(0,20),
     choose(0,9)].

g_pos_tstamp() -> elements([1,2,3,choose(0, ?POW_2(31))]).

g_posting() ->
    {g_i(), g_f(), g_t(), g_value(), g_props(), g_pos_tstamp()}.

g_postings() ->
    I = <<"index">>,
    F = <<"field">>,
    list(frequency([{10, {I,F,g_t(),g_value(),g_props(),g_pos_tstamp()}},
                    {1, g_posting()}])).

g_posting(Postings) ->
    case length(Postings) of
        0 ->
            g_posting();
        _ ->
            oneof([elements(Postings),
                   g_posting()])
    end.

g_range_query(Postings) ->
    case length(Postings) of
        0 ->
            {g_i(), g_f(), g_t(), g_t()};
        Len ->
            {I,F,ST,_,_,_} = lists:nth(random:uniform(Len), Postings),
            IFPostings = lists:filter(match(I,F), Postings),
            Len2 = length(IFPostings),
            {I,F,ET,_,_,_} = lists:nth(random:uniform(Len2), IFPostings),
            {ST1, ET1} = if ET < ST -> {ET, ST};
                            true -> {ST, ET}
                         end,
            elements([{I,F,ST1,ET1}, {I,F,ST1,undefined}, {I,F,undefined,ET1},
                      {I,F,undefined,undefined}])
    end.

%% ====================================================================
%% wrappers
%% ====================================================================

init([BRS,BDWS,BDWM,MCS,SQRAS,SCRAS,SFBSandSDWS,SDWM,SFRS,SBS,
      SVSS,SVCT,SVCL]) ->
    Root = "/tmp/test.test-test/prop_api",
    os:cmd(?FMT("rm -rf ~s; mkdir -p ~s", [Root, Root])),
    set(buffer_rollover_size, BRS),
    set(buffer_delayed_write_size, BDWS),
    set(buffer_delayed_write_ms, BDWM),
    set(max_compact_segments, MCS),
    set(segment_query_read_ahead_size, SQRAS),
    set(segment_compact_read_ahead_size, SCRAS),
    set(segment_file_buffer_size, SFBSandSDWS),
    set(segment_delayed_write_size, SFBSandSDWS),
    set(segment_delayed_write_ms, SDWM),
    set(segment_full_read_size, SFRS),
    set(segment_block_size, SBS),
    set(segment_values_staging_size, SVSS),
    set(segment_values_compression_threshold, SVCT),
    set(segment_values_compression_level, SVCL),

    merge_index:start_link(Root),
    {ok, Pid} = merge_index:start_link(Root),
    Pid.

index(Pid, Postings) ->
    merge_index:index(Pid, Postings).

info(Pid, {I,F,T,_,_,_}) ->
    merge_index:info(Pid, I, F, T).

is_empty(Pid) ->
    merge_index:is_empty(Pid).

fold(Pid, Fun, Acc) ->
    merge_index:fold(Pid, Fun, Acc).

lookup(Pid, {I,F,T,_,_,_}) ->
    Ft = fun(_,_) -> true end,
    merge_index:lookup(Pid, I, F, T, Ft).

lookup_sync(Pid, {I,F,T,_,_,_}) ->
    Ft = fun(_,_) -> true end,
    merge_index:lookup_sync(Pid, I, F, T, Ft).

range(Pid, {I, F, ST, ET}, Size) ->
    Ft = fun(_,_) -> true end,
    merge_index:range(Pid, I, F, ST, ET, Size, Ft).

range_sync(Pid, {I, F, ST, ET}, Size) ->
    Ft = fun(_,_) -> true end,
    merge_index:range_sync(Pid, I, F, ST, ET, Size, Ft).

drop(Pid) ->
    merge_index:drop(Pid).

compact(Pid) ->
    merge_index:compact(Pid).

%% ====================================================================
%% helpers
%% ====================================================================

set(Par, Val) ->
    application:set_env(merge_index, Par, Val).

fold_fun(I, F, T, V, P, TS, Acc) ->
    [{I, F, T, V, P, TS}|Acc].

unique_vals({V,P}, Acc) ->
    case orddict:find(V, Acc) of
        {ok, _} ->
            Acc;
        error ->
            orddict:store(V, P, Acc)
    end.

iterate(Itr) ->
    iterate(Itr(), []).

iterate(eof, Acc) ->
    lists:flatten(lists:reverse(Acc));
iterate({Res, Itr}, Acc) ->
    iterate(Itr(), [Res|Acc]).


match(I, F) ->
    fun({Ii,Ff,_,_,_,_}) ->
            Ii == I andalso Ff == F
    end.

-endif.
