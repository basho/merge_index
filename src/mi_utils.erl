%% -------------------------------------------------------------------
%%
%% mi: Merge-Index Data Store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(mi_utils).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-export([
         ets_keys/1,
         longest_prefix/2,
         edit_signature/2,
         hash_signature/1,
         fuzz/2
]).

ets_keys(Table) ->
    Key = ets:first(Table),
    ets_keys_1(Table, Key).
ets_keys_1(_Table, '$end_of_table') ->
    [];
ets_keys_1(Table, Key) ->
    [Key|ets_keys_1(Table, ets:next(Table, Key))].

%% longest_prefix/2 - Given two terms, calculate the longest common
%% prefix of the terms.
longest_prefix(A, B) when is_list(A) andalso is_list(B) ->
    longest_prefix_list(A, B, []);
longest_prefix(A, B) when is_binary(A) andalso is_binary(B) ->
    longest_prefix_binary(A, B, []);
longest_prefix(_, _) ->
    <<>>.

longest_prefix_list([C|A], [C|B], Acc) ->
    longest_prefix_list(A, B, [C|Acc]);
longest_prefix_list(_, _, Acc) ->
    lists:reverse(Acc).

longest_prefix_binary(<<C, A/binary>>, <<C, B/binary>>, Acc) ->
    longest_prefix_binary(A, B, [C|Acc]);
longest_prefix_binary(_, _, Acc) ->
    lists:reverse(Acc).

%% edit_signature/2 - Given an A term and a B term, return a bitstring
%% consisting of a 0 bit for each matching char and a 1 bit for each
%% non-matching char.
edit_signature(A, B) when is_binary(A) andalso is_binary(B) ->
    list_to_bitstring(edit_signature_binary(A, B));
edit_signature(A, B) when is_integer(A) ->
    edit_signature(<<A:32/integer>>, B);
edit_signature(A, B) when is_integer(B) ->
    edit_signature(A, <<B:32/integer>>);
edit_signature(_, _) ->
    <<>>.


edit_signature_binary(<<C, A/binary>>, <<C, B/binary>>) ->
    [<<0:1/integer>>|edit_signature_binary(A, B)];
edit_signature_binary(<<_, A/binary>>, <<_, B/binary>>) ->
    [<<1:1/integer>>|edit_signature_binary(A, B)];
edit_signature_binary(<<>>, <<_, B/binary>>) ->
    [<<1:1/integer>>|edit_signature_binary(<<>>, B)];
edit_signature_binary(_, <<>>) ->
    [].

%% hash_signature/1 - Given a term, repeatedly xor and rotate the bits
%% of the field to calculate a unique 1-byte signature. This is used
%% for speedier matches.
hash_signature(Term) when is_binary(Term)->
    hash_signature_binary(Term, 0);
hash_signature(Term) when is_integer(Term) ->
    hash_signature(<<Term:64/integer>>);
hash_signature(_Term) ->
    <<>>.

hash_signature_binary(<<C, Rest/binary>>, Acc) ->
    case Acc rem 2 of
        0 -> 
            RotatedAcc = ((Acc bsl 1) band 255),
            hash_signature_binary(Rest, RotatedAcc bxor C);
        1 -> 
            RotatedAcc = (((Acc bsl 1) + 1) band 255),
            hash_signature_binary(Rest, RotatedAcc bxor C)
    end;
hash_signature_binary(<<>>, Acc) ->
    Acc.

%% Add some random variation (plus or minus 25%) to the rollover size
%% so that we don't get all buffers rolling over at the same time.
fuzz(Value, FuzzPercent) ->
    Scale = 1 + (((random:uniform(100) - 50)/100) * FuzzPercent * 2),
    Value * Scale.
