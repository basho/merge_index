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
    index/7, index/2, 
    stream/7,
    range/9,
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

%% @doc Start a new merge_index server.
-spec start_link(string()) -> {ok, Pid::pid()} | ignore | {error, Error::any()}.
start_link(Root) ->
    gen_server:start_link(mi_server, [Root], [{timeout, infinity}]).

%% @doc This is a no op...why is it here?
stop(_ServerPid) ->
    ok.

%% @doc Index the IFT with the supplied `Value', `Props' and
%% `Timestamp'.
index(ServerPid, Index, Field, Term, Value, Props, Timestamp) ->
    index(ServerPid, [{Index, Field, Term, Value, Props, Timestamp}]).

%% @doc Index `Postings'.
-spec index(pid(), [posting()]) -> ok.
index(ServerPid, Postings) ->
    gen_server:call(ServerPid, {index, Postings}, infinity).

%% @doc Return a `Weight' for the given IFT.
-spec info(pid(), index(), field(), mi_term()) ->
                  {ok, [{Term::any(), Weight::integer()}]}.
info(ServerPid, Index, Field, Term) ->
    gen_server:call(ServerPid, {info, Index, Field, Term}, infinity).

%% @doc Stream the results for IFT.
%%
%% `Pid' - The client, values will be streamed to this process.
%%
%% `Ref' - A unique reference for this request.
%%
%% `FilterFun' - Used to filter the results.
-spec stream(pid(), index(), field(), mi_term(), pid(), reference(), function()) ->
                    ok.
stream(ServerPid, Index, Field, Term, Pid, Ref, FilterFun) ->
    gen_server:call(ServerPid, 
        {stream, Index, Field, Term, Pid, Ref, FilterFun}, infinity).

%% @doc Much like `stream/7' except allows one to specify the range of
%% terms to stream over.  The range is a closed interval meaning that
%% both `StartTerm' and `EndTerm' are included.
%%
%% `StartTerm' - The start of the range.
%%
%% `EndTerm' - The end of the range.
%%
%% `Size' - The size of the term in bytes.
%%
%% @see stream/7.
-spec range(pid(), index(), field(), mi_term(), mi_term(),
            size(), pid(), reference(), function()) -> ok.
range(ServerPid, Index, Field, StartTerm, EndTerm, Size, Pid, Ref, FilterFun) ->
    gen_server:call(ServerPid, 
        {range, Index, Field, StartTerm, EndTerm, Size, Pid, Ref, FilterFun}, infinity).

%% @doc Predicate to determine if the buffers AND segments are empty.
-spec is_empty(pid()) -> boolean().
is_empty(ServerPid) ->
    gen_server:call(ServerPid, is_empty, infinity).

%% @doc Fold over all IFTs in the index.
%%
%% `Fun' - Function to fold over data.  The 1st arg is the posting and
%% the 2nd arg is the accumulator.
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
