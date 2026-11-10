# SQLite ownership and provenance

Moved without source changes from `crunchybananas/MCPCore` commit
`615081f14217d6d367aeb5b6e81068ac3aa58b1a` (1.6.0) on November 9, 2026.

- `sqlite3.c`: SQLite 3.47.2 amalgamation, retaining its embedded copyright disclaimer.
- `include/CSQLiteShim.h` and `include/module.modulemap`: MCPCore's SDK-header bridge, distributed under that repository's MIT license (Copyright (c) 2026 Crunchy Bananas, LLC; full notice in LICENSE).
- C build definitions, zlib linkage, and system-header/module-map behavior are unchanged.
- sqlite-vec remains separately documented in `../SQLiteVec/UPSTREAM.md`.

SHA-256:

```
b947b84b8c45d1513ecf2df8f46bf099d9f6d7a0acac0928061ddbb4ea6fe54b  sqlite3.c
4627011d851a01044adca45e9316c29b7abbb1d5cd04a267ea1e5a7f8ed37682  include/CSQLiteShim.h
3f20bdd7368447ce04d3a3768934323a9782face31efc1740f926544c66ef437  include/module.modulemap
```

This transfer changes package ownership, not the SQLite version, database schema,
or vector implementation. System-header compatibility and runtime behavior must
still be checked when changing SDKs or updating SQLite. RAGCore initializes its
statically linked sqlite-vec implementation directly; no dynamic extension download
is introduced by this move.
