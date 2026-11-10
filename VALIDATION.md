# Storage ownership validation

November 9, 2026. RAGCore branch `codex/own-sqlite`, based on `e222712028cb04ca2fa0e18e343a2317d20749b6`; paired with MCPCore cleanup commit `a86e4f3`.

- `swift test`: XCTest passed 50 tests; Swift Testing reported 127 tests across 25 suites, with the opt-in 10k-vector benchmark skipped. No failures. Coverage includes sqlite-vec initialization/KNN migration, FTS5 search, schema recovery, indexing and storage integrity.
- `swift build -c release`: passed after the final bridge header change (109.45 seconds).
- Separate Swift consumer imported MCPCore, RAGCore and RAGCore's CSQLite product together; JSON-RPC round trip, SQLite 3.47.2 runtime, FTS5/RTREE compile options, sqlite-vec v0.1.9 initialization and database reopen all passed. Reproducible source: INTEGRATION_CHECK.md.
- `swift package show-dependencies --format json`: parsed recursively; no MCPCore dependency in RAGCore's graph.
- Compared all three transferred CSQLite files byte-for-byte with MCPCore 1.6.0 commit `615081f`; all match. SHA-256 values and original license are preserved in Sources/CSQLite.
- Compiled syntax of the transferred SQLite amalgamation and vector bridge against the arm64 iOS 26 target using the installed iPhoneOS SDK and package defines. This found and fixed the bridge's missing `<stddef.h>` include for `NULL`. It is not a full iOS app build/device test.
- `git diff --check`: passed.

Existing compiler warnings in vendored C/ASTChunker sources remain. The SQLite iOS compile also reports its existing disabled `gethostuuid()` path. No Python tooling, model download, dynamic SQLite extension download, user database modification, or app launch was used. No release or tag was published. Peel's source and lockfiles remain unchanged; its coordinated app build has not been run. The local Peel source-lease runtime is not installed, so no shared-checkout edits were attempted.

Disk and retention: roughly 165 GiB free before RAGCore work, 164 GiB after, above the 30 GiB reserve. The disposable `.build/sqlite-boundary-check` consumer, its build cache and test database were removed after preserving source and results. The normal RAGCore `.build` cache and compact `.build/sqlite-ownership-*` logs remain for subsequent package review/builds; review their retention when this migration is accepted or retired. No release archive was created. The RAGCore checkout is the maintained source project, not a disposable clone.
