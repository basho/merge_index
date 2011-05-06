-module(mi_segment_tests).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POW_2(N), trunc(math:pow(2, N))).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

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
              {oneof(IFTs), oneof(IFTs)}, (End >= Start) andalso (I1 =:= I2) andalso (F1 =:= F2)).

g_value() ->
    non_empty(binary()).

g_props() ->
    list({oneof([word_pos, offset]), choose(0, ?POW_2(31))}).

g_tstamp() ->
    choose(0, ?POW_2(31)).

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
    ?FORALL(Entries, list({{g_i(), g_f(), g_t()}, g_value(), g_props(), g_tstamp()}),
            begin
                [file:delete(X) || X <- filelib:wildcard(filename:dirname(Root) ++ "/*")],

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

test_spec(Root, PropertyFn) ->
    {timeout, 60, fun() ->
                          application:load(merge_index),
                          os:cmd(?FMT("rm -rf ~s; mkdir -p ~s", [Root, Root])),
                          ?assert(eqc:quickcheck(eqc:numtests(250, ?QC_OUT(PropertyFn(Root ++ "/t1")))))
                  end}.
