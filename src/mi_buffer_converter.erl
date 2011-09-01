%% -------------------------------------------------------------------
%%
%% mi_buffer_converter: supervises transformation of a buffer 
%%                      into a segment file.
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
-module(mi_buffer_converter).

%% API
-export([start_link/3, convert/3]).

%% callbacks
-export([convert/4]).

%%====================================================================
%% API
%%====================================================================

start_link(Server, Root, Buffer) ->
    proc_lib:start_link(?MODULE, convert, [self(), Server, Root, Buffer], 1000).

convert(Server, Root, Buffer) ->
    mi_buffer_converter_sup:start_child(Server, Root, Buffer).

%%====================================================================
%% Callbacks
%%====================================================================

convert(Parent, Server, Root, Buffer) ->
    proc_lib:init_ack(Parent, {ok, self()}),
    try
        SNum  = mi_buffer:id(Buffer),
        SName = filename:join(Root, "segment." ++ integer_to_list(SNum)),

        case mi_server:has_deleteme_flag(SName) of
            true ->
                %% remove files from a previously-failed conversion
                file:delete(mi_segment:data_file(SName)),
                file:delete(mi_segment:offsets_file(SName));
            false ->
                mi_server:set_deleteme_flag(SName)
        end,
        SegmentWO = mi_segment:open_write(SName),
        mi_segment:from_buffer(Buffer, SegmentWO),
        mi_server:buffer_to_segment(Server, Buffer, SegmentWO),
        exit(normal)
    catch
        error:Reason ->
            case mi_buffer:exists(Buffer) of
                false ->
                    lager:warning("conversion for buffer ~p failed, probably"
                                  " because the buffer has been dropped ~p",
                                  [mi_buffer:filename(Buffer),
                                   erlang:get_stacktrace()]),
                    exit(normal);
                true ->
                    lager:error("conversion for buffer ~p failed with trace ~p",
                                [mi_buffer:filename(Buffer),
                                 erlang:get_stacktrace()]),
                    exit({error, Reason})
            end
    end.
