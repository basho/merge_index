-module(merge_index_tests).
-compile(export_all).
-import(common, [g_i/0, g_f/0, g_t/0, g_props/0, g_value/0]).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("common.hrl").

-record(state, {server_pid,
                postings=[]}).

prop_api_test_() ->
    {timeout, 60,
     fun() ->
            ?assert(eqc:quickcheck(eqc:numtests(200,?QC_OUT(prop_api()))))
     end
    }.

prop_api() ->
    application:load(merge_index),
    ok = application:start(sasl),
    %% Comment out following line to be spammed with SASL reports
    error_logger:delete_report_handler(sasl_report_tty_h),
    ok = application:start(merge_index),

    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S, Res} = run_commands(?MODULE, Cmds),

                case Res of
                    ok -> ok;
                    _ -> io:format(user,
                                   "QC History: ~p~n"
                                   "QC State: ~p~n"
                                   "QC result: ~p~n",
                                   [H, S, Res])
                end,

                Pid = S#state.server_pid,
                if Pid /= undefined -> merge_index:stop(Pid);
                   true -> ok
                end,

                aggregate(command_names(Cmds), Res == ok)
            end).

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
           {call,?MODULE,stream, [P, g_posting(Postings)]}]).

next_state(S, Pid, {call,_,init,_}) ->
    S#state{server_pid=Pid};
next_state(S, _Res, {call,_,index,[_,Postings]}) ->
    case Postings of
        [] -> S;
        _ ->
            Postings0 = S#state.postings,
            S#state{postings=Postings0++Postings}
    end;
next_state(S, _Res, {call,_,_,_}) -> S.

precondition(_,_) ->
    true.

postcondition(S, {call,_,is_empty,_}, V) ->
    case S#state.postings of
        [] -> V =:= true;
        _ -> V =:= false
    end;
postcondition(_, {call,_,index,_}, V) ->
    ok == ?assertEqual(ok, V);
postcondition(#state{postings=Postings}, {call,_,info,[_,I,F,T]}, V) ->
    L = [x || {Ii, Ff, Tt} <- Postings,
              (I == Ii) andalso (F == Ff) andalso (T == Tt)],
    {ok, [{T, W}]} = V,
    ok == ?assertEqual(length(L), W);
postcondition(#state{postings=Postings}, {call,_,fold,_}, {ok, V}) ->
    L = [{I,F,T,V,-1*TS,P} || {I,F,T,V,P,TS} <- Postings],
    %% NOTE: The order in which fold returns postings is not
    %% deterministic thus both must be sorted.
    ok == ?assertEqual(lists:sort(Postings), lists:sort(V));
postcondition(#state{postings=Postings},
              {call,_,stream,[_,{I,F,T,_,_,_}]}, V) ->
    L = [{V,P} || {Ii,Ff,Tt,V,P,_} <- Postings,
                  (I == Ii) andalso (F == Ff) andalso (T == Tt)],
    ok == ?assertEqual(L, V);
postcondition(_,_,_) -> true.

%% ====================================================================
%% generators
%% ====================================================================

g_size() ->
    choose(64, 1024).

g_settings() ->
    [g_size(), g_size()].

g_pos_tstamp() ->
    choose(0, ?POW_2(31)).

g_posting() ->
    {g_i(), g_f(), g_t(), g_value(), g_props(), g_pos_tstamp()}.

g_postings() ->
    list(g_posting()).

g_posting(Postings) ->
    case length(Postings) of
        0 ->
            g_posting();
        _ ->
            oneof([elements(Postings),
                   g_posting()])
    end.

%% ====================================================================
%% wrappers
%% ====================================================================

init([BRS,BDWS]) ->
    Root = "/tmp/test/prop_api",
    os:cmd(?FMT("rm -rf ~s; mkdir -p ~s", [Root, Root])),
    set(buffer_rollover_size, BRS),
    set(buffer_delayed_write_size, BDWS),
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

stream(Pid, {I,F,T,_,_,_}) ->
    Ref = make_ref(),
    Sink = spawn(?MODULE, data_sink, [Ref, [], false]),
    Ft = fun(_,_) -> true end,
    ok = merge_index:stream(Pid, I, F, T, Sink, Ref, Ft),
    wait_for_it(Sink, Ref).

%% ====================================================================
%% helpers
%% ====================================================================

set(Par, Val) ->
    application:set_env(merge_index, Par, Val).

fold_fun(I, F, T, V, P, TS, Acc) ->
    [{I, F, T, V, P, TS}|Acc].

data_sink(Ref, Acc, Done) ->
    receive
        {result_vec, Data, Ref} ->
            data_sink(Ref, Acc++Data, false);
        {result, '$end_of_table', Ref} ->
            data_sink(Ref, Acc, true);
        {gimmie, From, Ref} ->
            From ! {ok, Ref, lists:reverse(Acc)};
        {'done?', From, Ref} ->
            From ! {ok, Ref, Done},
            data_sink(Ref, Acc, Done);
        Other ->
            ?debugFmt("Unexpected msg: ~p~n", [Other]),
            data_sink(Ref, Acc, Done)
    end.

wait_for_it(Sink, Ref) ->
    S = self(),
    Sink ! {'done?', S, Ref},
    receive
        {ok, Ref, true} ->
            Sink ! {gimmie, S, Ref},
            receive
                {ok, Ref, Data} ->
                    Data;
                {ok, ORef, _} ->
                    ?debugFmt("Received data for older run: ~p~n", [ORef])
            end;
        {ok, Ref, false} ->
            timer:sleep(100),
            wait_for_it(Sink, Ref);
        {ok, ORef, _} ->
            ?debugFmt("Received data for older run: ~p~n", [ORef])
    end.
