%%======================================================================
%%
%% Leo Dcerl - [D]isc [C]ache [Erl]ang
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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
%% ---------------------------------------------------------------------
%% DCerl Server
%% @doc
%% @end
%%======================================================================
-module(leo_dcerl_server).
-author("Yosuke Hara").

-behaviour(gen_server).

-include("leo_dcerl.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/5, stop/1,
         get_filepath/2, get_ref/2, get/2, get/3,
         put/3, put/4, put_begin_tran/2, put_end_tran/5,
         delete/2, stats/1, items/1, size/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {handler :: #dcerl_state{},
                total_cache_size = 0 :: integer(),
                stats_gets       = 0 :: integer(),
                stats_puts       = 0 :: integer(),
                stats_dels       = 0 :: integer(),
                stats_hits       = 0 :: integer()
               }).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: {ok,Pid} | ignore | {error, Error}
%% Description: Starts the server.
-spec start_link(atom(), string(), string(), integer(), integer()) ->
                        'ignore' | {'error',_} | {'ok',pid()}.
start_link(Id, DataDir, JournalDir, CacheSize, ChunkSize) ->
    gen_server:start_link({local, Id}, ?MODULE,
                          [DataDir, JournalDir, CacheSize, ChunkSize], []).


%% Function: -> ok
%% Description: Manually stops the server.
stop(Pid) ->
    gen_server:cast(Pid, stop).


%% @doc Retrieve a reference
%%
-spec(get_ref(atom(), binary()) ->
             undefined | binary() | {error, any()}).
get_ref(Id, Key) ->
    gen_server:call(Id, {get_ref, Key}).


%% @doc Retrieve a value associated with a specified key
%%
-spec(get(atom(), binary()) ->
             undefined | binary() | {error, any()}).
get(Id, Key) ->
    gen_server:call(Id, {get, Key}).

%% @doc Retrieve a value associated with a specified key
%%
-spec(get_filepath(atom(), binary()) ->
             undefined | #cache_meta{} | {error, any()}).
get_filepath(Id, Key) ->
    gen_server:call(Id, {get_filepath, Key}).

%% @doc Retrieve a value associated with a specified key
%%
-spec(get(atom(), any(), binary()) ->
             undefined | binary() | {error, any()}).
get(Id, Ref, Key) ->
    gen_server:call(Id, {get, Ref, Key}).


%% @doc Insert a key-value pair into the leo_dcerl
%%
-spec(put(atom(), binary(), binary()) ->
             ok | {error, any()}).
put(Id, Key, Value) ->
    gen_server:call(Id, {put, Key, Value}).

-spec(put(atom(), any(), binary(), binary()) ->
             ok | {error, any()}).
put(Id, Ref, Key, Value) ->
    gen_server:call(Id, {put, Ref, Key, Value}).


%% @doc Start transaction of insert chunked objects
-spec(put_begin_tran(atom(), binary()) ->
             ok | {error, any()}).
put_begin_tran(Id, Key) ->
    gen_server:call(Id, {put_begin_tran, Key}).


%% @doc End transaction of insert chunked objects
-spec(put_end_tran(atom(), any(), binary(), #cache_meta{}, boolean()) ->
             ok | {error, any()}).
put_end_tran(Id, Ref, Key, Meta, IsCommit) ->
    gen_server:call(Id, {put_end_tran, Ref, Key, Meta, IsCommit}).


%% @doc Remove a key-value pair by a specified key into the leo_dcerl
-spec(delete(atom(), binary()) ->
             ok | {error, any()}).
delete(Id, Key) ->
    gen_server:call(Id, {delete, Key}).


%% @doc Return server's state
-spec(stats(atom()) ->
             any()).
stats(Id) ->
    gen_server:call(Id, {stats}).


%% @doc Return server's items
-spec(items(atom()) ->
             any()).
items(Id) ->
    gen_server:call(Id, {items}).


%% @doc Return server's summary of cache size
-spec(size(atom()) ->
             any()).
size(Id) ->
    gen_server:call(Id, {size}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
init([DataDir, JournalDir, CacheSize, ChunkSize]) ->
    case leo_dcerl:start(DataDir, JournalDir, CacheSize, ChunkSize) of
        {ok, Handler} ->
            {ok, #state{total_cache_size = CacheSize,
                        handler          = Handler}};
        {error, Cause} ->
            {stop, Cause, null}
    end.

handle_call({get_ref, Key}, _From, #state{handler = Handler} = State) ->
    {Res, NewState} =
        case catch leo_dcerl:get(Handler, Key) of
            {ok, Handler1, Ref} when is_record(Ref, dcerl_fd) ->
                {{ok, Ref}, State#state{handler = Handler1}};
            _ ->
                {{error, undefined}, State}
        end,
    {reply, Res, NewState};

handle_call({get, Key}, _From, #state{handler    = Handler,
                                      stats_gets = Gets,
                                      stats_hits = Hits} = State) ->
    {Res, NewState} =
        case catch leo_dcerl:get(Handler, Key) of
            {ok, Handler1, Ret} when is_binary(Ret) ->
                {{ok, Ret}, State#state{stats_gets = Gets + 1,
                                        stats_hits = Hits + 1,
                                        handler = Handler1}};
            {ok, Handler1, Ret} when is_record(Ret, dcerl_fd) ->
                case get_chunk_sub(Handler1, Ret) of
                    {ok, Handler2, Bin} ->
                        {{ok, Bin}, State#state{stats_gets = Gets + 1,
                                                stats_hits = Hits + 1,
                                                handler = Handler2}};
                    Error ->
                        {Error, State#state{handler = Handler1}}
                end;
            {not_found, Handler1} ->
                {not_found, State#state{stats_gets = Gets + 1,
                                        handler = Handler1}};
            {'EXIT', Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State};
            {error, Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State}
        end,
    {reply, Res, NewState};

handle_call({get_filepath, Key}, _From, #state{handler    = Handler,
                                               stats_gets = Gets,
                                               stats_hits = Hits} = State) ->
    {Res, NewState} =
        case catch leo_dcerl:get_filepath(Handler, Key) of
            {ok, Handler1, Ret} ->
                {{ok, Ret}, State#state{stats_gets = Gets + 1,
                                        stats_hits = Hits + 1,
                                        handler = Handler1}};
            {not_found, Handler1} ->
                {not_found, State#state{stats_gets = Gets + 1,
                                        handler = Handler1}};
            {'EXIT', Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State};
            {error, Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State}
        end,
    {reply, Res, NewState};

handle_call({get, Ref,_Key}, _From, #state{handler    = Handler,
                                           stats_gets = Gets,
                                           stats_hits = Hits} = State) ->
    {Res, NewState} =
        case catch leo_dcerl:get_chunk(Handler, Ref) of
            {ok, _Handler2, _Ref, Value, false} ->
                {{ok, {Value, false}}, State};
            {ok, Handler2, _Ref, Value, true} ->
                {{ok, {Value, true}}, State#state{handler = Handler2,
                                                  stats_gets = Gets + 1,
                                                  stats_hits = Hits + 1}};
            {'EXIT', Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State};
            {error, Cause} = Ret ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {Ret, State}
        end,
    {reply, Res, NewState};

handle_call({put, Key, Val}, _From, #state{handler = Handler,
                                           stats_puts = Puts} = State) ->
    {Res, NewState} =
        case catch leo_dcerl:put(Handler, Key, Val) of
            {ok, Handler1} ->
                {ok, State#state{stats_puts = Puts + 1,
                                 handler = Handler1}};
            {'EXIT', Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State};
            {error, Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State}
        end,
    {reply, Res, NewState};


handle_call({put, Ref,_Key, Val}, _From, #state{handler = Handler} = State) ->
    {Res, NewState} =
        case catch leo_dcerl:put_chunk(Handler, Ref, Val) of
            ok ->
                {ok, State};
            {'EXIT', Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State};
            {error, Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State}
        end,
    {reply, Res, NewState};

handle_call({put_begin_tran, Key}, _From, #state{handler = Handler} = State) ->
    {Res, NewState} =
        case catch leo_dcerl:put_begin(Handler, Key) of
            {ok, Handler2, Ref} ->
                {{ok, Ref}, State#state{handler = Handler2}};
            {'EXIT', Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State};
            {error, Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State}
        end,
    {reply, Res, NewState};

handle_call({put_end_tran, Ref, _Key, Meta, IsCommit}, _From, #state{handler = Handler,
                                                                     stats_puts = Puts} = State) ->
    {Res, NewState} =
        case catch leo_dcerl:put_end(Handler, Ref, Meta, IsCommit) of
            {ok, Handler2} ->
                {ok, State#state{handler = Handler2,
                                 stats_puts = Puts + 1}};
            {'EXIT', Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State};
            {error, Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State}
        end,
    {reply, Res, NewState};


handle_call({delete, Key}, _From, #state{handler    = Handler,
                                         stats_dels = Dels} = State) ->
    {Res, NewState} =
        case catch leo_dcerl:remove(Handler, Key) of
            {ok, Handler1} ->
                {ok, State#state{stats_dels = Dels + 1,
                                 handler = Handler1}};
            {'EXIT', Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State};
            {error, Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, State}
        end,
    {reply, Res, NewState};


handle_call({stats}, _From, #state{handler    = Handler,
                                   stats_hits = Hits,
                                   stats_gets = Gets,
                                   stats_puts = Puts,
                                   stats_dels = Dels} = State) ->
    {ok, Items} = lru:items(Handler#dcerl_state.cache_entries),
    {ok, DStats} = leo_dcerl:stats(Handler),
    Size = DStats#cache_stats.cached_size,

    Stats = #cache_stats{hits        = Hits,
                         gets        = Gets,
                         puts        = Puts,
                         dels        = Dels,
                         records     = Items,
                         cached_size = Size},
    {reply, {ok, Stats}, State};

handle_call({items}, _From, #state{handler = Handler} = State) ->
    Reply = lru:items(Handler#dcerl_state.cache_entries),
    {reply, Reply, State};

handle_call({size}, _From, #state{handler = Handler} = State) ->
    {ok, DStats} = leo_dcerl:stats(Handler),
    Size = DStats#cache_stats.cached_size,
    {reply, {ok, Size}, State};

handle_call(_Request, _From, State) ->
    {reply, undefined, State}.

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.


%% ----------------------------------------------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to terminate. When it returns,
%% the gen_server terminates with Reason. The return value is ignored.
%% ----------------------------------------------------------------------------------------------------------
terminate(_Reason, _State) ->
    terminated.


%% ----------------------------------------------------------------------------------------------------------
%% Function: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed.
%% ----------------------------------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Retrieve binary
%% @private
get_chunk_sub(Handler, Ref) ->
    get_chunk_sub(Handler, Ref, <<>>).

get_chunk_sub(Handler, Ref, Acc) ->
    Ret = case catch leo_dcerl:get_chunk(Handler, Ref) of
              {ok, Handler2, _Ref, Value, false} ->
                  {ok, {Handler2, Value, false}};
              {ok, Handler2, _Ref, Value, true} ->
                  {ok, {Handler2, Value, true}};
              {'EXIT', Cause} ->
                  error_logger:error_msg("~p,~p,~p,~p~n",
                                         [{module, ?MODULE_STRING},
                                          {function, "get_chunk_sub/3"},
                                          {line, ?LINE}, {body, Cause}]),
                  {error, Cause};
              {error, Cause} ->
                  error_logger:error_msg("~p,~p,~p,~p~n",
                                         [{module, ?MODULE_STRING},
                                          {function, "get_chunk_sub/3"},
                                          {line, ?LINE}, {body, Cause}]),
                  {error, Cause}
          end,

    case Ret of
        {ok, {Handler3, Chunk, false}} ->
            get_chunk_sub(Handler3, Ref, << Acc/binary, Chunk/binary >>);
        {ok, {Handler3, Chunk, true}} ->
            {ok, Handler3, << Acc/binary, Chunk/binary>>};
        {error, Reason} ->
            {error, Reason}
    end.
