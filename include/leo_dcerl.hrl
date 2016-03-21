%%======================================================================
%%
%% Leo Disk Cache Library for Erlang(leo_dcerl)
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%%======================================================================

-ifdef(namespaced_types).
-type dcerl_dict() :: dict:dict().
-type dcerl_set() :: sets:set().
-else.
-type dcerl_dict() :: dict().
-type dcerl_set() :: set().
-endif.

-record(cache_meta, {
          size = 0  :: non_neg_integer(),
          md5 = 0  :: integer(),
          mtime = 0  :: non_neg_integer(),
          content_type = "NULL" :: string() | binary(),
          file_path = "" :: file:name_all()
         }).

-record(cache_stats, {
          gets = 0 :: non_neg_integer(),
          puts = 0 :: non_neg_integer(),
          dels = 0 :: non_neg_integer(),
          hits = 0 :: non_neg_integer(),
          records = 0 :: non_neg_integer(),
          cached_size = 0 :: non_neg_integer()
         }).

%% For chunked read/write I/F
-record(dcerl_fd, {
          key = <<>> :: binary(),
          tmp_data_file_io_dev :: file:io_device()
         }).

%% dcerl's inner state
-record(dcerl_state, {
          journa_dir_path = [] :: string(),
          journal_file_io_dev = undefined :: file:io_device()|undefined,
          tmp_data_file_io_dev = undefined :: file:io_device()|undefined,
          data_dir_path = "" :: string(),
          max_cache_size = 0  :: pos_integer(),
          chunk_size = 64 :: pos_integer(),
          redundant_op_cnt = 0  :: non_neg_integer(),
          ongoing_keys :: dcerl_set(),
          cache_metas :: dcerl_dict(),
          cache_stats = #cache_stats{} :: #cache_stats{},
          cache_entries :: term() % NIF resource
         }).

-define(JOURNAL_FNAME, "journal").
-define(JOURNAL_MAGIC, "erlang.dcerl\n").
-define(JOURNAL_SEP, " \n").
-define(JOURNAL_OP_READ, "READ").
-define(JOURNAL_OP_CLEAN, "CLEAN").
-define(JOURNAL_OP_DIRTY, "DIRTY").
-define(JOURNAL_OP_REMOVE, "REMOVE").
-define(JOURNAL_MAX_RED_OP_CNT, 5).
-define(READMODE_READSIZE, 4194304).
