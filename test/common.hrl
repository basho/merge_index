-define(POW_2(N), trunc(math:pow(2, N))).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).
-export([unique_latest/2]).

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
              {oneof(IFTs), oneof(IFTs)},
              (End >= Start) andalso (I1 =:= I2) andalso (F1 =:= F2)).

g_value() ->
    non_empty(binary()).

g_props() ->
    list({oneof([word_pos, offset]), choose(0, ?POW_2(31))}).

%% Generate inverted tstamps to match mi_server
g_tstamp() ->
    choose(-?POW_2(31), 0).

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
    Acc = fold_iterator(Itr, Fun, Acc0),
    fold_iterators(Itrs, Fun, Acc).

%% Remember, tstamps are inverted by mi_server.
unique_latest({{Index, Field, Term}, Value, Tstamp, Props}, Acc) ->
    Key = {Index, Field, Term, Value},
    case orddict:find(Key, Acc) of
        {ok, {ExistingTstamp, _}} when Tstamp < ExistingTstamp ->
            orddict:store(Key, {Tstamp, Props}, Acc);
        error ->
            orddict:store(Key, {Tstamp, Props}, Acc);
        _ ->
            Acc
    end.

test_spec(Root, PropertyFn) ->
    test_spec(Root, PropertyFn, 250).

test_spec(Root, F, Runs) ->
    {timeout, 30,
     fun() ->
             application:load(merge_index),
             os:cmd(?FMT("rm -rf ~s; mkdir -p ~s", [Root, Root])),
             R = eqc:quickcheck(
                   eqc:numtests(Runs, ?QC_OUT(F(Root ++ "/t1")))),
             %% I know this looks weird but it's so that you know
             %% which `F` failed.
             {name, Name} = erlang:fun_info(F, name),
             ?assertEqual({Name, true}, {Name, R})
     end}.

