-module(mi_buffer_tests).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("common.hrl").

prop_basic_test(Root) ->
    ?FORALL(Entries, list({g_ift(), g_value(), g_props(), g_tstamp()}),
            begin
                check_entries(Root, Entries)
            end).

prop_dups_test(Root) ->
    ?FORALL(Entries, list(default({{<<0>>,<<0>>,<<0>>},<<0>>,[],0},
                                  {g_ift(), g_value(), g_props(), g_tstamp()})),
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
