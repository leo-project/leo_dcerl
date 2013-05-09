%% -------------------------------------------------------------------
%%
%% Leo Dcerl - Benchmarking Suite
%%
%% Copyright (c) 2013 Rakuten, Inc.
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
-module(basho_bench_driver_leo_dcerl).

-export([new/1,
         run/4]).

-record(state, 
        {handler         :: any(),
         chunk_size      :: non_neg_integer(),
         eprof           :: boolean(),
         check_integrity :: boolean()}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    case code:which(leo_dcerl) of
        non_existing ->
            io:format("leo-dcerl-benchmark requires leo_dcerl to be available on code path.\n");
        _ ->
            void
    end,

    CacheCapacity = basho_bench_config:get(
                      cache_capacity, 1073741824), %% default:1GB
    io:format("Cache capacity: ~w\n", [CacheCapacity]),
    CacheChunkSize = basho_bench_config:get(
                      cache_chunk_size, 1024 * 1024),
    io:format("Cache chunk_size: ~w\n", [CacheChunkSize]),
    CacheDataDir = basho_bench_config:get(
                      cache_data_dir, "work/d/") ++ integer_to_list(Id) ++ "/",
    io:format("Cache data_dir: ~w\n", [CacheDataDir]),
    CacheJournalDir = basho_bench_config:get(
                      cache_journal_dir, "work/j/") ++ integer_to_list(Id) ++ "/",
    io:format("Cache journal_dir: ~w\n", [CacheJournalDir]),
    CI = basho_bench_config:get(
                      check_integrity, false), %% should be false when doing benchmark
    io:format("Check Integrity: ~p\n", [CI]),
    Prof = basho_bench_config:get(
                      eprof, false),
    io:format("Eprof: ~p\n", [Prof]),
    case Prof of
        true ->
            eprof:start(),
            eprof:start_profiling([self()]);
        false -> void
    end,

    {ok, D} = leo_dcerl:start(
            CacheDataDir, CacheJournalDir, CacheCapacity, CacheChunkSize),
    {ok, #state{handler = D, chunk_size = CacheChunkSize, check_integrity = CI, eprof = Prof}}.

%terminate(_, #state{eprof = Prof} = _State) ->
run(eprof, _KeyGen, _ValueGen, #state{eprof = Prof} = State) ->
    case Prof of
        true ->
            eprof:stop_profiling(),
            {ok, Dir} = file:get_cwd(),
            PLog = filename:join(Dir, "proc.eprof"),
            io:format("Eprof proc log: ~p\n", [PLog]),
            eprof:log(PLog),
            eprof:analyze(procs),
            TLog = filename:join(Dir, "total.eprof"),
            io:format("Eprof total log: ~p\n", [TLog]),
            eprof:log(TLog),
            eprof:analyze(total),
            eprof:stop();
        false -> void
    end,
    {ok, State#state{eprof = false}};

run(get, KeyGen, _ValueGen, #state{handler = D, check_integrity = CI} = State) ->
    Key = KeyGen(),
    BinKey = list_to_binary(Key),
    case leo_dcerl:get(D, BinKey) of
        {ok, D2, Value} ->
            case CI of
                true ->
                    LocalMD5 = erlang:get(BinKey),
                    RemoteMD5 = erlang:md5(Value),
                    case RemoteMD5 =:= LocalMD5 of
                        true -> {ok, State};
                        false -> {error, checksum_error}
                    end;
                false -> {ok, State#state{handler = D2}}
            end;
        {not_found, D2}->
            {ok, State#state{handler = D2}};
        {error, Reason} ->
            {error, Reason}
    end;

run(put, KeyGen, ValueGen, #state{handler = D, check_integrity = CI} = State) ->
    Key = KeyGen(),
    Val = ValueGen(),
    BinKey = list_to_binary(Key),
    case leo_dcerl:put(D, BinKey, Val) of
        {ok, D2} ->
            case CI of
                true ->
                    LocalMD5 = erlang:md5(Val),
                    erlang:put(BinKey, LocalMD5);
                false -> void
            end,
            {ok, State#state{handler = D2}};
        {error, Reason} ->
            {error, Reason}
    end.
