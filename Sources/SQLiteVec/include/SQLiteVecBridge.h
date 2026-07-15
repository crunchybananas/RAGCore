#ifndef SQLITE_VEC_BRIDGE_H
#define SQLITE_VEC_BRIDGE_H

#include <stdint.h>
#include <sqlite3.h>

/// Initialize the statically linked sqlite-vec extension on one connection.
/// Returns a SQLite result code and may populate error_message.
int32_t sqlite_vec_initialize(sqlite3 *database, char **error_message);

/// The sqlite-vec version compiled into this package.
const char *sqlite_vec_compiled_version(void);

#endif
