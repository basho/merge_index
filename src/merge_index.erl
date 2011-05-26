%% -------------------------------------------------------------------
%%
%% @doc The merge_index applicaiton is an index from Index/Field/Term
%% (IFT) tuples to Values.  The values are document IDs or some form
%% of identification for the object which contains the IFT.
%% Futhermore, each IFT/Value pair has an associated proplists (Props)
%% and timestamp (Timestamp) which are used to describe where the IFT
%% was found in the Value and at what time the entry was written,
%% respectively.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc. All Rights Reserved.
%% @end
%% -------------------------------------------------------------------
-module(merge_index).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-include_lib("kernel/include/file.hrl").

-export([
    %% API
    start_link/1,
    stop/1,
    index/2,
    lookup/5, lookup_sync/5,
    range/7, range_sync/7,
    info/4,
    is_empty/1,
    fold/3,
    drop/1,
    compact/1
]).

-type(index() :: any()).
-type(field() :: any()).
-type(mi_term() :: any()).
-type(size() :: all | integer()).
-type(posting() :: {Index::index(),
                    Field::field(),
                    Term::mi_term(),
                    Value::any(),
                    Props::[proplists:property()],
                    Timestamp::integer()}).
-type(iterator() :: fun(() -> {any(), iterator()} | eof)).

-define(LOOKUP_TIMEOUT, 60000).

%% @doc Start a new merge_index server.
-spec start_link(string()) -> {ok, Pid::pid()} | ignore | {error, Error::any()}.
start_link(Root) ->
    gen_server:start_link(mi_server, [Root], [{timeout, infinity}]).

%% @doc Stop the merge_index server.
-spec stop(pid()) -> ok.
stop(ServerPid) ->
    mi_server:stop(ServerPid).

%% @doc Index `Postings'.
-spec index(pid(), [posting()]) -> ok.
index(ServerPid, Postings) ->
    gen_server:call(ServerPid, {index, Postings}, infinity).

%% @doc Return a `Weight' for the given IFT.
-spec info(pid(), index(), field(), mi_term()) ->
                  {ok, [{Term::any(), Weight::integer()}]}.
info(ServerPid, Index, Field, Term) ->
    gen_server:call(ServerPid, {info, Index, Field, Term}, infinity).

%% @doc Lookup the results for IFT and return an iterator.  This
%% allows the caller to process data as it comes in/wants it.
%%
%% @throws lookup_timeout
%%
%% `Server' - Pid of the server instance.
%%
%% `Filter' - Function used to filter the results.
-spec lookup(pid(), index(), field(), mi_term(), function()) -> iterator().
lookup(Server, Index, Field, Term, Filter) ->
    {ok, Ref} = mi_server:lookup(Server, Index, Field, Term, Filter),
    make_result_iterator(Ref).

%% @doc Lookup the results for IFT and return a list.  The caller will
%% block until the result list is built.
%%
%% @throws lookup_timeout
%%
%% `Server' - Pid of the server instance.
%%
%% `Filter' - Function used to filter the results.
-spec lookup_sync(pid(), index(), field(), mi_term(), function()) -> list().
lookup_sync(Server, Index, Field, Term, Filter) ->
    {ok, Ref} = mi_server:lookup(Server, Index, Field, Term, Filter),
    make_result_list(Ref).

%% @doc Much like `lookup' except allows one to specify a range of
%% terms.  The range is a closed interval meaning that both
%% `StartTerm' and `EndTerm' are included.
%%
%% `StartTerm' - The start of the range.
%%
%% `EndTerm' - The end of the range.
%%
%% `Size' - The size of the term in bytes.
%%
%% @see lookup/5.
-spec range(pid(), index(), field(), mi_term(), mi_term(),
                 size(), function()) -> iterator().
range(Server, Index, Field, StartTerm, EndTerm, Size, Filter) ->
    {ok, Ref} = mi_server:range(Server, Index, Field, StartTerm, EndTerm,
                                Size, Filter),
    make_result_iterator(Ref).

%% @doc Much like `lookup_sync' except allows one to specify a range
%% of terms.  The range is a closed interval meaning that both
%% `StartTerm' and `EndTerm' are included.
%%
%% `StartTerm' - The start of the range.
%%
%% `EndTerm' - The end of the range.
%%
%% `Size' - The size of the term in bytes.
%%
%% @see lookup_sync/5.
-spec range_sync(pid(), index(), field(), mi_term(), mi_term(),
                 size(), function()) -> list().
range_sync(Server, Index, Field, StartTerm, EndTerm, Size, Filter) ->
    {ok, Ref} = mi_server:range(Server, Index, Field, StartTerm, EndTerm,
                                Size, Filter),
    make_result_list(Ref).

%% @doc Predicate to determine if the buffers AND segments are empty.
-spec is_empty(pid()) -> boolean().
is_empty(ServerPid) ->
    gen_server:call(ServerPid, is_empty, infinity).

%% @doc Fold over all IFTs in the index.
%%
%% `Fun' - Function to fold over data.  It takes 7 args.  1-6 are `I',
%% `F', `T', `Value', `Props', `Timestamp' and the 7th is the
%% accumulator.
%%
%% `Acc' - The accumulator to seed the fold with.
%%
%% `Acc2' - The final accumulator.
-spec fold(pid(), function(), any()) -> {ok, Acc2::any()}.
fold(ServerPid, Fun, Acc) ->
    gen_server:call(ServerPid, {fold, Fun, Acc}, infinity).

%% @doc Drop all current state and start from scratch.
-spec drop(pid()) -> ok.
drop(ServerPid) ->
    gen_server:call(ServerPid, drop, infinity).

%% @doc Perform compaction of segments if needed.
%%
%% `Segs' - The number of segments compacted.
%%
%% `Bytes' - The number of bytes compacted.
-spec compact(pid()) ->
                     {ok, Segs::integer(), Bytes::integer()}
                         | {error, Reason::any()}.
compact(ServerPid) ->
    gen_server:call(ServerPid, start_compaction, infinity).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @private
make_result_iterator(Ref) ->
    fun() -> result_iterator(Ref) end.

%% @private
result_iterator(Ref) ->
    receive
        {results, Results, Ref} ->
            {Results, fun() -> result_iterator(Ref) end};
        {eof, Ref} ->
            eof
    after
        ?LOOKUP_TIMEOUT ->
            throw(lookup_timeout)
    end.

%% @private
make_result_list(Ref) ->
    make_result_list(Ref, []).

%% @private
make_result_list(Ref, Acc) ->
    receive
        {results, Results, Ref} ->
            make_result_list(Ref, [Results|Acc]);
        {eof, Ref} ->
            lists:flatten(lists:reverse(Acc))
    after
        ?LOOKUP_TIMEOUT ->
            throw(lookup_timeout)
    end.
