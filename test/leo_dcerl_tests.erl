-module(leo_dcerl_tests).
-author('yoshiyuki.kanno@stoic.co.jp').

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
    {ok, Dir} = file:get_cwd(),
    Src = init_source(),
    DataDir = filename:join(Dir, "data") ++ "/",
    JournalDir = filename:join(Dir, "journal") ++ "/",
    {ok, DS} = leo_dcerl:start(DataDir, JournalDir, 1024, 128),
    BinBody = data_block(Src, 128),
    BinKey = <<"test.com/b/path_to_file.jpg">>,
    {ok, DS2} = leo_dcerl:put(DS, BinKey, BinBody),
    {ok, CS} = leo_dcerl:stats(DS2),
    ?assertEqual(1, CS#cache_stats.puts),
    ?assertEqual(1, CS#cache_stats.records),
    {ok, DS3, BinBody} = leo_dcerl:get(DS2, BinKey),
    {ok, DS4} = leo_dcerl:remove(DS3, BinKey),
    {ok, CS2} = leo_dcerl:stats(DS4),
    ?assertEqual(1, CS2#cache_stats.dels),
    ?assertEqual(0, CS2#cache_stats.records),
    {ok, DS6, FD} = leo_dcerl:put_begin(DS4, BinKey),
    Chunk = data_block(Src, 128),
    ok  = leo_dcerl:put_chunk(DS6, FD, Chunk),
    ok  = leo_dcerl:put_chunk(DS6, FD, Chunk),
    ok  = leo_dcerl:put_chunk(DS6, FD, Chunk),
    CM = #cache_meta{
            md5 = 1,
            mtime = 123,
            content_type = "image/jpeg"},
    {ok, DS7} = leo_dcerl:put_end(DS6, FD, CM, true),
    {ok, CS3} = leo_dcerl:stats(DS7),
    ?assertEqual(2, CS3#cache_stats.puts),
    ?assertEqual(1, CS3#cache_stats.records),
    {ok, DS8, FD2} = leo_dcerl:get(DS7, BinKey),
    {ok, DS9} = get_chunked(DS8, FD2, Chunk),
    {ok, CS4} = leo_dcerl:stats(DS9),
    ?assertEqual(2, CS4#cache_stats.gets),
    ?assertEqual(2, CS4#cache_stats.hits),
    ?assertEqual(1, CS4#cache_stats.records),
    {ok, DS10, CM2} = leo_dcerl:get_filepath(DS9, BinKey),
    ?assertEqual(128*3, CM2#cache_meta.size),
    ?assertEqual(1, CM2#cache_meta.md5),
    ?assertEqual(123, CM2#cache_meta.mtime),
    ?assertEqual("image/jpeg", CM2#cache_meta.content_type),
    {ok, CS5} = leo_dcerl:stats(DS10),
    ?assertEqual(3, CS5#cache_stats.gets),
    ?assertEqual(3, CS5#cache_stats.hits),
    ?assertEqual(1, CS5#cache_stats.records),
    {ok, _} = leo_dcerl:delete(DS10),
    ok.

roll_test() ->
    {ok, Dir} = file:get_cwd(),
    Src = init_source(),
    DataDir = filename:join(Dir, "data") ++ "/",
    JournalDir = filename:join(Dir, "journal") ++ "/",
    {ok, DS} = leo_dcerl:start(DataDir, JournalDir, 1024, 512),
    BinBody = data_block(Src, 384),
    BinKey = <<"test.com/b/path_to_file.jpg">>,
    {ok, DS2} = leo_dcerl:put(DS, BinKey, BinBody),
    {ok, DS3} = leo_dcerl:put(DS2, <<BinKey/binary, <<".1">>/binary >>, BinBody),
    {ok, DS4} = leo_dcerl:put(DS3, <<BinKey/binary, <<".2">>/binary >>, BinBody),
    {not_found, DS5} = leo_dcerl:get(DS4, BinKey),  
    {ok, CS} = leo_dcerl:stats(DS5),
    ?assertEqual(2, CS#cache_stats.records),
    {ok, DS6, BinBody} = leo_dcerl:get(DS5, <<BinKey/binary, <<".1">>/binary >>),  
    {ok, DS7} = leo_dcerl:put(DS6, <<BinKey/binary, <<".3">>/binary >>, BinBody),
    {not_found, DS8} = leo_dcerl:get(DS7, <<BinKey/binary, <<".2">>/binary >>),  
    {ok, CS2} = leo_dcerl:stats(DS8),
    ?assertEqual(2, CS2#cache_stats.records),
    {ok, _} = leo_dcerl:delete(DS8),
    ok.

recover_test() ->
    {ok, Dir} = file:get_cwd(),
    Src = init_source(),
    DataDir = filename:join(Dir, "data") ++ "/",
    JournalDir = filename:join(Dir, "journal") ++ "/",
    io:format(user,"[debug] d:~p j:~p~n", [DataDir, JournalDir]),
    {ok, DS} = leo_dcerl:start(DataDir, JournalDir, 1024, 512),
    BinBody = data_block(Src, 384),
    BinKey = <<"test.com/b/path_to_file.jpg">>,
    {ok, DS2} = leo_dcerl:put(DS, BinKey, BinBody),
    {ok, DS3} = leo_dcerl:put(DS2, <<BinKey/binary, <<".1">>/binary >>, BinBody),
    {ok, DS4} = leo_dcerl:put(DS3, <<BinKey/binary, <<".2">>/binary >>, BinBody),
    leo_dcerl:stop(DS4),
    {ok, DS5} = leo_dcerl:start(DataDir, JournalDir, 1024, 512),
    {ok, CS} = leo_dcerl:stats(DS5),
    ?assertEqual(2, CS#cache_stats.records),
    {ok, _} = leo_dcerl:delete(DS5),
    ok.

get_chunked(DS, FD, Chunk) ->
    case leo_dcerl:get_chunk(DS, FD) of
        {ok, DS2, FD, Chunk, false} ->
            get_chunked(DS2, FD, Chunk);
        {ok, DS2, _FD, <<>>, true} ->
            {ok, DS2}
    end.

-endif.
