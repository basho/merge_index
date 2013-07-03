%% -------------------------------------------------------------------
%%
%% mi_buffer: ordered index based on ETS tables, stored in memory.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(mi_buffer).
-author("Rusty Klophaus <rusty@basho.com>").
-include("merge_index.hrl").
-export([
    new/1,
    filename/1,
    id/1,
    exists/1,
    close_filehandle/1,
    delete/1,
    filesize/1,
    size/1,
    write/2,
    info/4,
    iterator/1, iterator/4, iterators/6
]).

-record(buffer, {
    filename,
    handle,
    table,
    size
}).

%%% Creates a disk-based append-mode buffer file with support for a
%%% sorted iterator.

%% Open a new buffer. Returns a buffer structure.
new(Filename) ->
    %% Open the existing buffer file...
    filelib:ensure_dir(Filename),
    %% {ok, DelayedWriteSize} = application:get_env(merge_index, buffer_delayed_write_size),
    %% {ok, DelayedWriteMS} = application:get_env(merge_index, buffer_delayed_write_ms),
    %% FuzzedWriteSize = trunc(mi_utils:fuzz(DelayedWriteSize, 0.1)),
    %% FuzzedWriteMS = trunc(mi_utils:fuzz(DelayedWriteMS, 0.1)),
    {ok, FH} = file:open(Filename, [read, write, raw, binary]),

    %% Read into an ets table...
    Table = ets:new(buffer, [duplicate_bag, public]),
    open_inner(FH, Table, Filename),
    {ok, Size} = file:position(FH, cur),

    lager:debug("opened buffer '~s'", [Filename]),
    %% Return the buffer.
    #buffer { filename=Filename, handle=FH, table=Table, size=Size }.

open_inner(FH, Table, Filename) ->
    case read_value(FH, Filename) of
        {ok, Postings} ->
            write_to_ets(Table, Postings),
            open_inner(FH, Table, Filename);
        eof ->
            ok
    end.

filename(Buffer) -> Buffer#buffer.filename.

id(#buffer{filename=Filename}) -> id(Filename);
id(Filename) -> list_to_integer(tl(filename:extension(Filename))).

exists(#buffer{table=Table}) -> ets:info(Table) /= undefined.

delete(Buffer=#buffer{table=Table, filename=Filename}) ->
    ets:delete(Table),
    close_filehandle(Buffer),
    file:delete(Filename),
    file:delete(Filename ++ ".deleted"),
    lager:debug("deleted buffer '~s'", [Filename]),
    ok.

close_filehandle(Buffer) ->
    file:close(Buffer#buffer.handle).

%% Return the current size of the buffer file.
filesize(Buffer) ->
    Buffer#buffer.size.

size(Buffer) ->
    ets:info(Buffer#buffer.table, size).

write(Postings, Buffer) ->
    %% Write to file...
    FH = Buffer#buffer.handle,
    BytesWritten = write_to_file(FH, Postings),

    %% Return a new buffer with a new tree and size...
    write_to_ets(Buffer#buffer.table, Postings),

    %% Return the new buffer.
    Buffer#buffer {
        size = (BytesWritten + Buffer#buffer.size)
    }.

%% Return the number of results under this IFT.
info(Index, Field, Term, Buffer) ->
    Table = Buffer#buffer.table,
    Key = {Index, Field, Term},
    length(ets:lookup(Table, Key)).

%% Return an iterator that traverses the entire buffer.
iterator(Buffer) ->
    Table = Buffer#buffer.table,
    List1 = lists:sort(ets:tab2list(Table)),
    List2 = [{I,F,T,V,K,P} || {{I,F,T},V,K,P} <- List1],
    fun() -> iterate_list(List2) end.

%% Return an iterator that traverses the values for a term in the buffer.
iterator(Index, Field, Term, Buffer) ->
    Table = Buffer#buffer.table,
    List1 = ets:lookup(Table, {Index, Field, Term}),
    List2 = [{V,K,P} || {_Key,V,K,P} <- List1],
    List3 = lists:sort(List2),
    fun() -> iterate_list(List3) end.

%% Return a list of iterators over a range.
iterators(Index, Field, StartTerm, EndTerm, Size, Buffer) ->
    Table = Buffer#buffer.table,
    Keys = mi_utils:ets_keys(Table),
    Filter = gen_filter(Index, Field, StartTerm, EndTerm, Size),
    MatchingKeys = lists:filter(Filter, Keys),
    [iterator(I,F,T, Buffer) || {I,F,T} <- MatchingKeys].

%% Turn a list into an iterator.
iterate_list([]) ->
    eof;
iterate_list([H|T]) ->
    {H, fun() -> iterate_list(T) end}.


%% ===================================================================
%% Internal functions
%% ===================================================================

read_value(FH, Filename) ->
    {ok, CurrPos} = file:position(FH, cur),
    case file:read(FH, 4) of
        {ok, <<Size:32/unsigned-integer>>} when Size > 0->
            case file:read(FH, Size) of
                {ok, <<B:Size/binary>>} ->
                    {ok, binary_to_term(B)};
                _ ->
                    log_truncation(Filename, CurrPos),
                    file:position(FH, {bof, CurrPos}),
                    eof
            end;
        {ok, _Binary}  ->
            log_truncation(Filename, CurrPos),
            file:position(FH, {bof, CurrPos}),
            eof;
        eof ->
            eof
    end.

log_truncation(Filename, Position) ->
    error_logger:warning_msg("Corrupted posting detected in ~s after reading ~w bytes, ignoring remainder.",
                          [Filename, Position]).

write_to_file(FH, Terms) when is_list(Terms) ->
    %% Convert all values to binaries, count the bytes.
    B = term_to_binary(Terms),
    Size = erlang:size(B),
    Bytes = <<Size:32/unsigned-integer, B/binary>>,
    file:write(FH, Bytes),
    Size + 2.

write_to_ets(Table, Postings) ->
    ets:insert(Table, Postings).

%% @private
%% @doc Given and Index, Field, StartTerm, EndTerm, and Size, return a
%%      filter function that returns true if the provided Key (of
%%      format {Index, Field, Term}) is within the acceptable range.
-spec gen_filter(merge_index:index(), merge_index:field(),
                 merge_index:mi_term(), merge_index:mi_term(),
                 merge_index:size()) ->
                        fun((merge_index:index(), merge_index:field(), merge_index:mi_term()) -> boolean()).
gen_filter(Index, Field, StartTerm, EndTerm, Size) ->
    %% Construct a function to check start bounds...
    StartFun = case StartTerm of
                   undefined ->
                       fun({KeyIndex, KeyField, _}) ->
                               {KeyIndex, KeyField} >= {Index, Field}
                       end;
                   _ ->
                       fun(Key) ->
                               Key >= {Index, Field, StartTerm}
                       end
               end,

    %% Construct a function to check end bounds...
    EndFun = case EndTerm of
                   undefined ->
                       fun({KeyIndex, KeyField, _}) ->
                               {KeyIndex, KeyField} =< {Index, Field}
                       end;
                   _ ->
                       fun(Key) ->
                               Key =< {Index, Field, EndTerm}
                       end
               end,

    %% Possibly construct a function to check size. Return the final
    %% filter function...
    case Size of
        all ->
            fun(Key) -> StartFun(Key) andalso EndFun(Key) end;
        _ ->
            SizeFun = fun({_, _, KeyTerm}) -> erlang:size(KeyTerm) == Size end,
            fun(Key) -> StartFun(Key) andalso EndFun(Key) andalso SizeFun(Key) end
    end.
