#include "SQLiteVecBridge.h"

#include "sqlite-vec.h"

int32_t sqlite_vec_initialize(sqlite3 *database, char **error_message) {
  return sqlite3_vec_init(database, error_message, NULL);
}

const char *sqlite_vec_compiled_version(void) {
  return SQLITE_VEC_VERSION;
}
