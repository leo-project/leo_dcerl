-author('yoshiyuki.kanno@stoic.co.jp').

-record(cache_meta, {
    size         = 0  :: non_neg_integer(),
    md5          = 0  :: integer(),
    mtime        = 0  :: non_neg_integer(),
    content_type = "NULL" :: string() | binary(),
    file_path    = "" :: file:name_all()
}).

-record(cache_stats, {
    gets        = 0 :: non_neg_integer(),
    puts        = 0 :: non_neg_integer(),
    dels        = 0 :: non_neg_integer(),
    hits        = 0 :: non_neg_integer(),
    records     = 0 :: non_neg_integer(),
    cached_size = 0 :: non_neg_integer()
}).

%% For chunked read/write I/F
-record(dcerl_fd, {
    key                = <<>> :: binary(),
    tmp_datafile_iodev        :: file:io_device()
}).

%% dcerl's inner state
-record(dcerl_state, {
    journaldir_path    = ""        :: string(),
    journalfile_iodev  = undefined :: file:io_device()|undefined,
    tmp_datafile_iodev = undefined :: file:io_device()|undefined,
    datadir_path       = ""        :: string(),
    max_cache_size     = 0         :: pos_integer(),
    chunk_size         = 64        :: pos_integer(),
    redundant_op_cnt   = 0         :: non_neg_integer(),
    ongoing_keys                   :: set(),
    cache_metas                    :: dict(),
    cache_stats                    :: #cache_stats{},
    cache_entries                  :: term() % NIF resource
}).

-define(JOURNAL_FNAME, "journal").
-define(JOURNAL_MAGIC, "erlang.dcerl\n").
-define(JOURNAL_SEP, " \n").
-define(JOURNAL_OP_READ,   "READ").
-define(JOURNAL_OP_CLEAN,  "CLEAN").
-define(JOURNAL_OP_DIRTY,  "DIRTY").
-define(JOURNAL_OP_REMOVE, "REMOVE").
-define(JOURNAL_MAX_RED_OP_CNT, 5).
