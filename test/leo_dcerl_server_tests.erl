%%======================================================================
%%
%% DCerl - [D]isc [C]ache [Erl]ang
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
%% DCerl Server TEST
%% @doc
%% @end
%%======================================================================
-module(leo_dcerl_server_tests).
-author("Yosuke Hara").

-include("leo_dcerl.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).
%% gen test data
init_source() ->
    SourceSz = 1024 * 1024,
    {SourceSz, crypto:rand_bytes(SourceSz)}.

data_block({SourceSz, Source}, BlockSize) ->
    case SourceSz - BlockSize > 0 of
        true ->
            Offset = random:uniform(SourceSz - BlockSize),
            <<_:Offset/bytes, Slice:BlockSize/bytes, _Rest/binary>> = Source,
            Slice;
        false ->
            Source
    end.

normal_test() ->
    %% Launch Server
    {ok, Dir} = file:get_cwd(),
    Src = init_source(),

    Id = 'test_dcerl',
    DataDir = filename:join(Dir, "data") ++ "/",
    JournalDir = filename:join(Dir, "journal") ++ "/",
    {ok, _Pid} = leo_dcerl_server:start_link(Id, DataDir, JournalDir, 1024, 128),

    %% Test - Put#1
    BinBody = data_block(Src, 128),
    BinKey = <<"test.com/b/path_to_file.jpg">>,
    ok = leo_dcerl_server:put(Id, BinKey, BinBody),

    {ok, CS} = leo_dcerl_server:stats(Id),
    ?assertEqual(1, CS#cache_stats.puts),
    ?assertEqual(1, CS#cache_stats.records),

    %% Test - Get/Delete
    {ok, BinBody} = leo_dcerl_server:get(Id, BinKey),
    ok = leo_dcerl_server:delete(Id, BinKey),
    {ok, CS2} = leo_dcerl_server:stats(Id),
    ?assertEqual(1, CS2#cache_stats.dels),
    ?assertEqual(0, CS2#cache_stats.records),

    %% Test - PUT#2
    {ok, Ref} = leo_dcerl_server:put_begin_tran(Id, BinKey),
    {error, conflict} = leo_dcerl_server:put_begin_tran(Id, BinKey),
    Chunk = data_block(Src, 128),
    ok = leo_dcerl_server:put(Id, Ref, BinKey, Chunk),
    ok = leo_dcerl_server:put(Id, Ref, BinKey, Chunk),
    ok = leo_dcerl_server:put(Id, Ref, BinKey, Chunk),
    CM = #cache_meta{
            md5 = 1,
            mtime = 123,
            content_type = "image/jpeg"},
    ok = leo_dcerl_server:put_end_tran(Id, Ref, BinKey, CM, true),

    {ok, CS3} = leo_dcerl_server:stats(Id),
    ?assertEqual(2, CS3#cache_stats.puts),
    ?assertEqual(1, CS3#cache_stats.records),

    %% Test - Get#2/Delete#2
    {ok, Bin2} = leo_dcerl_server:get(Id, BinKey),
    ?assertEqual((128*3), byte_size(Bin2)),

    {ok, Ref2}= leo_dcerl_server:get_ref(Id, BinKey),
    ok = get_chunked(Id, Ref2, BinKey, Chunk),

    {ok, CM2} = leo_dcerl_server:get_filepath(Id, BinKey),
    ?assertEqual(128*3, CM2#cache_meta.size),
    ?assertEqual(1, CM2#cache_meta.md5),
    ?assertEqual(123, CM2#cache_meta.mtime),
    ?assertEqual("image/jpeg", CM2#cache_meta.content_type),

    {ok, Items} = leo_dcerl_server:items(Id),
    {ok, Size}  = leo_dcerl_server:size(Id),
    ?assertEqual(1, Items),
    ?assertEqual((128*3), Size),

    {ok, CS4} = leo_dcerl_server:stats(Id),
    ?assertEqual(4, CS4#cache_stats.gets),
    ?assertEqual(4, CS4#cache_stats.hits),
    ?assertEqual(1, CS4#cache_stats.records),
    ok = leo_dcerl_server:delete(Id, BinKey),
    ok.

get_chunked(Id, Ref, Key, Chunk) ->
    case leo_dcerl_server:get(Id, Ref, Key) of
        {ok, {Chunk, false}} ->
            get_chunked(Id, Ref, Key, Chunk);
        {ok, {<<>>, true}} ->
            ok
    end.

-endif.
