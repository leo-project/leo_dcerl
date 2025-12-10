# leo_dcerl

leo_dcerl is a disk-based SSD-aware caching library for Erlang.

## Requirements

- Erlang/OTP 22 or later (tested on OTP 25, 26, 27, 28)
- rebar3
- CMake
- C compiler

## Build

```bash
$ rebar3 compile
```

## Test

```bash
$ rebar3 eunit
```

## Usage

### Basic API

```erlang
%% Start cache
DataDir = "/path/to/data/",
JournalDir = "/path/to/journal/",
MaxCacheSize = 1024 * 1024 * 100,  %% 100MB
ChunkSize = 64 * 1024,              %% 64KB
{ok, State} = leo_dcerl:start(DataDir, JournalDir, MaxCacheSize, ChunkSize).

%% Put
Key = <<"my_key">>,
Value = <<"my_value">>,
{ok, State2} = leo_dcerl:put(State, Key, Value).

%% Get
{ok, State3, Value} = leo_dcerl:get(State2, Key).

%% Remove
{ok, State4} = leo_dcerl:remove(State3, Key).

%% Stop
{ok, _} = leo_dcerl:stop(State4).
```

### Using gen_server API

```erlang
%% Start server
Id = my_cache,
{ok, _Pid} = leo_dcerl_server:start_link(Id, DataDir, JournalDir, MaxCacheSize, ChunkSize).

%% Put
ok = leo_dcerl_server:put(Id, Key, Value).

%% Get
{ok, Value} = leo_dcerl_server:get(Id, Key).

%% Delete
ok = leo_dcerl_server:delete(Id, Key).

%% Stats
{ok, Stats} = leo_dcerl_server:stats(Id).
```

### Chunked Read/Write

For large objects, use chunked API:

```erlang
%% Chunked write
{ok, State2, Fd} = leo_dcerl:put_begin(State, Key),
ok = leo_dcerl:put_chunk(State2, Fd, Chunk1),
ok = leo_dcerl:put_chunk(State2, Fd, Chunk2),
CacheMeta = #cache_meta{md5 = MD5, mtime = MTime, content_type = ContentType},
{ok, State3} = leo_dcerl:put_end(State2, Fd, CacheMeta, true).

%% Chunked read
{ok, State4, Fd2} = leo_dcerl:get(State3, Key),
{ok, State5, Fd2, Chunk, false} = leo_dcerl:get_chunk(State4, Fd2),
%% Continue reading until Tail = true
```

## License

leo_dcerl's license is [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

## Copyright

Copyright (c) 2012-2018 Rakuten, Inc.

Copyright (c) 2019-2025 Lions Data, Ltd.
