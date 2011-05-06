-module(mi_buffer_tests).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POW_2(N), trunc(math:pow(2, N))).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

g_iftv() ->
    non_empty(binary()).

g_i() ->
    non_empty(binary()).

g_f() ->
    non_empty(binary()).

g_t() ->
    non_empty(binary()).

g_ift() ->
    {g_i(), g_f(), g_t()}.

g_value() ->
    non_empty(binary()).

g_props() ->
    list({oneof([word_pos, offset]), choose(0, ?POW_2(31))}).

g_tstamp() ->
    choose(0, ?POW_2(31)).

g_ift_range(IFTs) ->
    ?SUCHTHAT({{I1, F1, _T1}=Start, {I2, F2, _T2}=End},
              {oneof(IFTs), oneof(IFTs)}, (End >= Start) andalso (I1 =:= I2) andalso (F1 =:= F2)).

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

prop_basic_test(Root) ->
    ?FORALL(Entries, list({{g_iftv(), g_iftv(), g_iftv()}, g_iftv(), g_props(), g_tstamp()}),
            begin
                check_entries(Root, Entries)
            end).

prop_dups_test(Root) ->
    ?FORALL(Entries, list(default({{<<0>>,<<0>>,<<0>>},<<0>>,[],0},
                                  {{g_iftv(), g_iftv(), g_iftv()}, g_iftv(), g_props(), g_tstamp()})),
            begin
                check_entries(Root, Entries)
            end).

check_entries(Root, Entries) ->
    [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],
    Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),

    L1 = [{I, F, T, Value, Props, Tstamp}
          || {{I, F, T}, Value, Props, Tstamp} <- Entries],

    L2 = fold_iterator(mi_buffer:iterator(Buffer),
                       fun(Item, Acc0) -> [Item | Acc0] end, []),
    mi_buffer:delete(Buffer),
    equals(lists:sort(L1), lists:sort(L2)).

prop_iter_range_test(Root) ->
    ?LET({I, F}, {g_i(), g_f()},
         ?LET(IFTs, non_empty(list(frequency([{10, {I, F, g_t()}}, {1, g_ift()}]))),
              ?FORALL({Entries, Range},
                      {list({oneof(IFTs), g_value(), g_props(), g_tstamp()}), g_ift_range(IFTs)},
                      begin check_range(Root, Entries, Range) end))).

check_range(Root, Entries, Range) ->
    [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],
    Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),

    {Start, End} = Range,
    {Index, Field, StartTerm} = Start,
    {Index, Field, EndTerm} = End,
    Itrs = mi_buffer:iterators(Index, Field, StartTerm, EndTerm, all, Buffer),
    L1 = fold_iterators(Itrs, fun(Item, Acc0) -> [Item | Acc0] end, []),

    L2 = [{V, K, P}
          || {Ii, Ff, Tt, V, K, P} <- fold_iterator(mi_buffer:iterator(Buffer),
                                                    fun(I,A) -> [I|A] end, []),
             {Ii, Ff, Tt} >= Start, {Ii, Ff, Tt} =< End],
    mi_buffer:delete(Buffer),
    equals(lists:sort(L1), lists:sort(L2)).

prop_basic_test_() ->
    test_spec("/tmp/test/mi_buffer_basic", fun prop_basic_test/1).

prop_dups_test_() ->
    test_spec("/tmp/test/mi_buffer_basic", fun prop_dups_test/1).

prop_iter_range_test_() ->
    test_spec("/tmp/test/mi_buffer_iter", fun prop_iter_range_test/1).

test_spec(Root, PropertyFn) ->
    {timeout, 60, fun() ->
                          application:load(merge_index),
                          os:cmd(?FMT("rm -rf ~s; mkdir -p ~s", [Root, Root])),
                          ?assert(eqc:quickcheck(eqc:numtests(250, ?QC_OUT(PropertyFn(Root ++ "/t1")))))
                  end}.
