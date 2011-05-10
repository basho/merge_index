-module(mi_buffer_tests).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("common.hrl").

prop_basic_test(Root) ->
    ?FORALL(Entries, list({g_ift(), g_value(), g_tstamp(), g_props()}),
            begin
                check_entries(Root, Entries)
            end).

prop_dups_test(Root) ->
    ?FORALL(Entries, list(default({{<<0>>,<<0>>,<<0>>},<<0>>,[],0},
                                  {g_ift(), g_value(), g_tstamp(), g_props()})),
            begin
                check_entries(Root, Entries)
            end).

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
    ?LET({I, F}, {g_i(), g_f()},
         ?LET(IFTs, non_empty(list(frequency([{10, {I, F, g_t()}}, {1, g_ift()}]))),
              ?FORALL({Entries, Range},
                      {list({oneof(IFTs), g_value(), g_tstamp(), g_props()}), g_ift_range(IFTs)},
                      begin check_range(Root, Entries, Range) end))).

check_range(Root, Entries, Range) ->
    Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),

    {Start, End} = Range,
    {Index, Field, StartTerm} = Start,
    {Index, Field, EndTerm} = End,

    L1 = [{V, K, P} || {{_, _, _}=IFT, V, K, P} <- Entries,
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
                      begin check_count(Root, Entries, IFT) end))).

check_count(Root, Entries, IFT) ->
    Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),

    {Ie, Fe, Te} = IFT,
    L1 = [X || {{I, F, T}, _, _, _} = X <- Entries,
               (I =:= Ie) andalso (F =:= Fe) andalso (T =:= Te)],

    C1 = length(L1),
    C2 = mi_buffer:info(Ie, Fe, Te, Buffer),
    mi_buffer:delete(Buffer),
    equals(C1, C2).

prop_basic_test_() ->
    test_spec("/tmp/test/mi_buffer_basic", fun prop_basic_test/1).

prop_dups_test_() ->
    test_spec("/tmp/test/mi_buffer_dups", fun prop_dups_test/1).

prop_iter_range_test_() ->
    test_spec("/tmp/test/mi_buffer_iter", fun prop_iter_range_test/1).

prop_info_test_() ->
    test_spec("/tmp/test/mi_buffer_info", fun prop_info_test/1).
