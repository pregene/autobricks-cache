#ifndef AUTOBRICKS_CACHE_H
#define AUTOBRICKS_CACHE_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Initializes the process-wide Cache runtime.
 *
 * connection_config and cache_config are UTF-8 JSON strings.
 *
 * Every function returning char * returns a UTF-8 JSON string owned by the
 * caller. Release it with ab_cache_string_free().
 */
char *ab_cache_initialize(
    const char *connection_config,
    const char *cache_config);

char *ab_cache_query(const char *cache_id, const char *input);
char *ab_cache_insert(const char *cache_id, const char *input);
char *ab_cache_update(const char *cache_id, const char *input);
char *ab_cache_delete(const char *cache_id, const char *input);
char *ab_cache_status(const char *cache_id);
char *ab_cache_uninitialize(void);

void ab_cache_string_free(char *value);

#ifdef __cplusplus
}
#endif

#endif
