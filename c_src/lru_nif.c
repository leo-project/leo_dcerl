#include "erl_nif.h"
#include "libcutil.h"

#define CHERLY_RES_TYPE "lru_res"

static ERL_NIF_TERM lru_nif_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lru_nif_stop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lru_nif_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lru_nif_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lru_nif_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lru_nif_eldest(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lru_nif_iterator(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lru_nif_iterator_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lru_nif_items(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifFunc nif_funcs[] =
  {
    {"start",  0, lru_nif_init},
    {"stop",   1, lru_nif_stop},
    {"get" ,   2, lru_nif_get},
    {"put" ,   3, lru_nif_put},
    {"remove", 2, lru_nif_remove},
    {"eldest", 1, lru_nif_eldest},
    {"iterator", 1, lru_nif_iterator},
    {"iterator_next", 1, lru_nif_iterator_next},
    {"items" , 1, lru_nif_items}
  };

static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_oom;
static ERL_NIF_TERM atom_not_found;

/**
 * Initialize
 */
static ERL_NIF_TERM lru_nif_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ERL_NIF_TERM term;
  ErlNifResourceType* pert;
  lcu_cache* cache;

  pert = (ErlNifResourceType*)enif_priv_data(env);
  cache = enif_alloc_resource(pert, sizeof(lcu_cache));

  term = enif_make_resource(env, cache);
  lcu_cache_init(cache, auto_eject_off, 8192 * 65536, 1.5, 16, 8192);
  enif_release_resource(cache);

  return enif_make_tuple2(env, atom_ok, term);
}


/**
 * Stop
 */
static ERL_NIF_TERM lru_nif_stop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  lcu_cache *cache;
  ErlNifResourceType* pert;

  if (argc < 1) {
    return enif_make_badarg(env);
  }

  pert = (ErlNifResourceType*)enif_priv_data(env);

  if (!enif_get_resource(env, argv[0], pert, (void**)&cache)) {
    return enif_make_badarg(env);
  }

  lcu_cache_destroy(cache);
  return atom_ok;
}


/**
 * Retrieve an cacheect from LRU-Storage
 */
static ERL_NIF_TERM lru_nif_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  lcu_cache *cache;
  String key;
  String val;

  ErlNifResourceType* pert;
  ErlNifBinary keybin;
  ErlNifBinary bin;

  if (argc < 2) {
    return enif_make_badarg(env);
  }

  pert = (ErlNifResourceType*)enif_priv_data(env);
  if (!enif_get_resource(env, argv[0], pert, (void**)&cache)) {
    return enif_make_badarg(env);
  }

  if (!enif_inspect_binary(env, argv[1], &keybin)) {
    return enif_make_badarg(env);
  }

  if (keybin.size <= 0) {
    return enif_make_badarg(env);
  }

  key.str = keybin.data;
  key.len = keybin.size;
  lcu_cache_get(cache, key, &val);

  if (val.str == NULL) {
    return atom_not_found;
  }

  if (!enif_alloc_binary(val.len, &bin)) {
    return enif_make_badarg(env);
  }

  memcpy(bin.data, val.str, val.len);
  return enif_make_tuple2(env, atom_ok, enif_make_binary(env, &bin));

}


/**
 * Insert an cacheect into LRU-Storage
 */
static ERL_NIF_TERM lru_nif_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  lcu_cache *cache;
  String key;
  String val;

  ErlNifResourceType* pert;
  ErlNifBinary keybin;
  ErlNifBinary bin;
  bool ret;

  if (argc < 2) {
    return enif_make_badarg(env);
  }

  pert = (ErlNifResourceType*)enif_priv_data(env);

  if (!enif_get_resource(env, argv[0], pert, (void**)&cache)) {
    return enif_make_badarg(env);
  }
  if (!enif_inspect_binary(env, argv[1], &keybin)) {
    return enif_make_badarg(env);
  }
  if (keybin.size <= 0) {
    return enif_make_badarg(env);
  }
  if (!enif_inspect_binary(env, argv[2], &bin)) {
    return enif_make_badarg(env);
  }
  key.str = keybin.data;
  key.len = keybin.size;
  val.str = bin.data;
  val.len = bin.size;
  ret = lcu_cache_put(cache, key, val);
  return ret ? atom_ok : enif_make_tuple2(env, atom_error, atom_oom);
}


/**
 * Remove an cacheect from LRU-Storage
 */
static ERL_NIF_TERM lru_nif_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  lcu_cache *cache;
  String key;
  ErlNifResourceType* pert;
  ErlNifBinary keybin;

  if (argc < 2) {
    return enif_make_badarg(env);
  }

  pert = (ErlNifResourceType*)enif_priv_data(env);

  if (!enif_get_resource(env, argv[0], pert, (void**)&cache)) {
    return enif_make_badarg(env);
  }

  if (!enif_inspect_binary(env, argv[1], &keybin)) {
    return enif_make_badarg(env);
  }
  if (keybin.size <= 0) {
    return enif_make_badarg(env);
  }

  key.str = keybin.data;
  key.len = keybin.size;
  lcu_cache_delete(cache, key);
  return atom_ok;
}

static ERL_NIF_TERM lru_nif_eldest(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  lcu_cache *cache;
  ErlNifResourceType* pert;
  ErlNifBinary keybin;
  ErlNifBinary bin;
  String key;
  String val;

  if (argc < 1) {
    return enif_make_badarg(env);
  }

  pert = (ErlNifResourceType*)enif_priv_data(env);

  if (!enif_get_resource(env, argv[0], pert, (void**)&cache)) {
    return enif_make_badarg(env);
  }

  lcu_cache_eldest(cache, &key, &val);
  if (key.str == NULL) {
    return atom_not_found;
  }
  if (!enif_alloc_binary(key.len, &keybin)) {
    return enif_make_badarg(env);
  }
  if (!enif_alloc_binary(val.len, &bin)) {
    return enif_make_badarg(env);
  }
  memcpy(keybin.data, key.str, key.len);
  memcpy(bin.data, val.str, val.len);
  return enif_make_tuple3(env, atom_ok, enif_make_binary(env, &keybin), enif_make_binary(env, &bin));
}

static ERL_NIF_TERM lru_nif_iterator(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  lcu_cache *cache;
  ErlNifResourceType* pert;
  ErlNifBinary keybin;
  ErlNifBinary bin;
  String key;
  String val;

  if (argc < 1) {
    return enif_make_badarg(env);
  }

  pert = (ErlNifResourceType*)enif_priv_data(env);

  if (!enif_get_resource(env, argv[0], pert, (void**)&cache)) {
    return enif_make_badarg(env);
  }

  lcu_cache_iterator(cache, &key, &val);
  if (key.str == NULL) {
    return atom_not_found;
  }
  if (!enif_alloc_binary(key.len, &keybin)) {
    return enif_make_badarg(env);
  }
  if (!enif_alloc_binary(val.len, &bin)) {
    return enif_make_badarg(env);
  }
  memcpy(keybin.data, key.str, key.len);
  memcpy(bin.data, val.str, val.len);
  return enif_make_tuple3(env, atom_ok, enif_make_binary(env, &keybin), enif_make_binary(env, &bin));
}

static ERL_NIF_TERM lru_nif_iterator_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  lcu_cache *cache;
  ErlNifResourceType* pert;
  ErlNifBinary keybin;
  ErlNifBinary bin;
  String key;
  String val;

  if (argc < 1) {
    return enif_make_badarg(env);
  }

  pert = (ErlNifResourceType*)enif_priv_data(env);

  if (!enif_get_resource(env, argv[0], pert, (void**)&cache)) {
    return enif_make_badarg(env);
  }
  lcu_cache_iterator_next(cache, &key, &val);
  if (key.str == NULL) {
    return atom_not_found;
  }
  if (!enif_alloc_binary(key.len, &keybin)) {
    return enif_make_badarg(env);
  }
  if (!enif_alloc_binary(val.len, &bin)) {
    return enif_make_badarg(env);
  }
  memcpy(keybin.data, key.str, key.len);
  memcpy(bin.data, val.str, val.len);
  return enif_make_tuple3(env, atom_ok, enif_make_binary(env, &keybin), enif_make_binary(env, &bin));
  
}

/**
 * Retrieve total of cacheects
 */
static ERL_NIF_TERM lru_nif_items(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  lcu_cache *cache;
  ErlNifResourceType* pert;
  ErlNifUInt64 len;

  if (argc < 1) {
    return enif_make_badarg(env);
  }

  pert = (ErlNifResourceType*)enif_priv_data(env);

  if (!enif_get_resource(env, argv[0], pert, (void**)&cache)) {
    return enif_make_badarg(env);
  }

  len = lcu_cache_item_size(cache);
  return enif_make_tuple2(env, atom_ok, enif_make_uint64(env, len));
}


/**
 * When calling onload or uggrade
 */
static int onload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
  ErlNifResourceFlags erf = ERL_NIF_RT_CREATE|ERL_NIF_RT_TAKEOVER;
  ErlNifResourceType* pert = enif_open_resource_type(env, NULL, CHERLY_RES_TYPE, NULL, erf, &erf);

  if (pert == NULL) {
    return 1;
  }

  *priv_data = (void*)pert;
  atom_ok = enif_make_atom(env, "ok");
  atom_error = enif_make_atom(env, "error");
  atom_oom = enif_make_atom(env, "oom");
  atom_not_found = enif_make_atom(env, "not_found");
  return 0;
}

/**
 *  Onload
 */
int lru_nif_onload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
  return onload(env, priv_data, load_info);
}


/**
 * Upgrade
 */
int lru_nif_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
  return onload(env, priv_data, load_info);
}


ERL_NIF_INIT(lru, nif_funcs, lru_nif_onload, NULL, lru_nif_upgrade, NULL)
