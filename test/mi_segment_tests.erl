-module(mi_segment_tests).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("common.hrl").

get_count(Entries) ->
    fun ({{I1, F1, T1}, _, _, _}) ->
            Fun = fun({I2, F2, T2, _, _, _}) ->
                          (I1 =:= I2) and (F1 =:= F2) and (T1 =:= T2)
                  end,
            length(lists:filter(Fun, Entries))
    end.

use_info(Segment) ->
    fun ({{I, F, T}, _, _, _}) ->
            mi_segment:info(I, F, T, Segment)
    end.

prop_basic_test(Root) ->
    ?FORALL({Entries, Size},
            {list({g_ift(), g_value(), g_props(), g_tstamp()}),
             choose(64,1024)},
            begin
                [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],
                application:set_env(merge_index, segment_full_read_size, Size),
                F = fun({{Index, Field, Term}, Value, Props, Tstamp}, Acc) ->
                            Key = {Index, Field, Term, Value},
                            case orddict:find(Key, Acc) of
                                {ok, {_, ExistingTstamp}} when Tstamp >= ExistingTstamp ->
                                    orddict:store(Key, {Props, Tstamp}, Acc);
                                error ->
                                    orddict:store(Key, {Props, Tstamp}, Acc);
                                _ ->
                                    Acc
                            end
                    end,
                L1 = [{Index, Field, Term, Value, Props, Tstamp} ||
                         {{Index, Field, Term, Value}, {Props, Tstamp}}
                             <- lists:foldl(F, [], Entries)],

                Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),
                mi_segment:from_buffer(Buffer, mi_segment:open_write(Root ++ "_segment")),
                Segment = mi_segment:open_read(Root ++ "_segment"),

                %% Fold over the entire segment
                SL = fold_iterator(mi_segment:iterator(Segment),
                                   fun(Item, Acc0) -> [Item | Acc0] end, []),

                Three = lists:sublist(Entries, 3),
                C1 = lists:map(get_count(L1), Three),
                C2 = lists:map(use_info(Segment), Three),

                mi_buffer:delete(Buffer),
                mi_segment:delete(Segment),
                conjunction([{entires, equals(L1, SL)},
                             {info, equals(true, C2 >= C1)}])
            end).

prop_iter_range_test(Root) ->
    ?LET({I, F}, {g_i(), g_f()},
         ?LET(IFTs, non_empty(list(frequency([{10, {I, F, g_t()}}, {1, g_ift()}]))),
              ?FORALL({Entries, Range},
                      {list({oneof(IFTs), g_value(), g_props(), g_tstamp()}), g_ift_range(IFTs)},
                      begin check_range(Root, Entries, Range) end))).

check_range(Root, Entries, Range) ->
    [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],
    Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),
    mi_segment:from_buffer(Buffer, mi_segment:open_write(Root ++ "_segment")),
    Segment = mi_segment:open_read(Root ++ "_segment"),

    {Start, End} = Range,
    {Index, Field, StartTerm} = Start,
    {Index, Field, EndTerm} = End,
    Itrs = mi_segment:iterators(Index, Field, StartTerm, EndTerm, all, Segment),
    L1 = fold_iterators(Itrs, fun(Item, Acc0) -> [Item | Acc0] end, []),

    L2 = [{V, K, P}
          || {Ii, Ff, Tt, V, K, P} <- fold_iterator(mi_segment:iterator(Segment),
                                                    fun(I,A) -> [I|A] end, []),
             {Ii, Ff, Tt} >= Start, {Ii, Ff, Tt} =< End],
    mi_buffer:delete(Buffer),
    mi_segment:delete(Segment),
    equals(lists:sort(L1), lists:sort(L2)).

prop_iter_test(Root) ->
    ?LET(IFT, {g_i(), g_f(), g_t()},
         ?LET(IFTs, non_empty(list(frequency([{10, IFT}, {1, g_ift()}]))),
              ?FORALL(Entries,
                      list({oneof(IFTs), g_value(), g_props(), g_tstamp()}),
                      begin check_iter(Root, Entries, IFT) end))).

check_iter(Root, Entries, IFT) ->
    [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],

    {I, F, T} = IFT,

    Fun = fun({{Index, Field, Term}, Value, Props, Tstamp}, Acc) ->
                  Key = {Index, Field, Term, Value},
                  case orddict:find(Key, Acc) of
                      {ok, {_, ExistingTstamp}} when Tstamp >= ExistingTstamp ->
                          orddict:store(Key, {Props, Tstamp}, Acc);
                      error ->
                          orddict:store(Key, {Props, Tstamp}, Acc);
                      _ ->
                          Acc
                  end
          end,
    L1 = [{Value, Props, Tstamp} ||
             {{Index, Field, Term, Value}, {Props, Tstamp}}
                 <- lists:foldl(Fun, [], Entries),
             Index =:= I, Field =:= F, Term =:= T],
    Buffer = mi_buffer:write(Entries, mi_buffer:new(Root ++ "_buffer")),
    mi_segment:from_buffer(Buffer, mi_segment:open_write(Root ++ "_segment")),
    Segment = mi_segment:open_read(Root ++ "_segment"),

    Itr = mi_segment:iterator(I, F, T, Segment),
    L2 = fold_iterator(Itr, fun(X, Acc) -> [X|Acc] end, []),

    mi_segment:delete(Segment),
    mi_buffer:delete(Buffer),
    equals(L1, L2).

prop_basic_test_() ->
    test_spec("/tmp/test/mi_segment_basic", fun prop_basic_test/1).

prop_iter_range_test_() ->
    test_spec("/tmp/test/mi_segment_iter_range", fun prop_iter_range_test/1).

prop_iter_test_() ->
    test_spec("/tmp/test/mi_segment_iter", fun prop_iter_test/1).
