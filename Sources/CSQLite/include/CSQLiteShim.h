// CSQLite shim — exposes the system sqlite3 headers through a distinct header
// so that importers see a stable CSQLite module whose type definitions match
// the macOS SDK that Xcode is building against.
//
// Previously this target vendored sqlite3.h + sqlite3ext.h from a specific
// SQLite release. That breaks whenever the macOS SDK's sqlite3_api_routines
// struct gains fields (e.g. `setlk_timeout` in macOS 26.3.1), because Swift's
// module verifier rejects the mismatched definitions between `SQLite3.Ext`
// (system) and `CSQLite` (vendored).
//
// Using the system headers via this shim keeps CSQLite always in sync with the
// SDK. The `[system] [extern_c]` attributes on the modulemap suppress the
// cross-module struct-equality check.

#ifndef CSQLITE_SHIM_H
#define CSQLITE_SHIM_H

#include <sqlite3.h>
#include <sqlite3ext.h>

// Apple's macOS SDK sqlite3.h defines `SQLITE_OMIT_LOAD_EXTENSION` by default
// and therefore elides the declarations for the extension-loading API. The
// symbols themselves ARE present in libsqlite3.dylib at runtime — Apple's
// build ships with extension support, they just hide the headers to nudge
// apps away from it. RAGCore (and any downstream importer that loads
// sqlite-vec, sqlite-vss, etc.) needs the declarations to compile.
//
// Re-declare them here so CSQLite downstream consumers can bridge them in
// Swift. No behavior change — these are just the stock prototypes.
int sqlite3_enable_load_extension(sqlite3 *db, int onoff);
int sqlite3_load_extension(sqlite3 *db, const char *zFile, const char *zProc, char **pzErrMsg);

#endif
