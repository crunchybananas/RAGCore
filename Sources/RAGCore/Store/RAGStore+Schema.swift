//
//  RAGStore+Schema.swift
//  RAGCore
//
//  Schema management and migrations (v1→v16).
//

import CSQLite
import Foundation

extension RAGStore {

  // MARK: - Database Lifecycle

  internal func openIfNeeded() throws {
    guard db == nil else { return }
    let dir = dbURL.deletingLastPathComponent()
    if !FileManager.default.fileExists(atPath: dir.path) {
      try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    }
    var handle: OpaquePointer?
    let result = sqlite3_open(dbURL.path, &handle)
    guard result == SQLITE_OK, let handle else {
      throw RAGError.sqlite("Cannot open database: \(result)")
    }
    db = handle

    // WAL mode for better concurrency
    try exec("PRAGMA journal_mode=WAL")
    try exec("PRAGMA busy_timeout=5000")
  }

  /// Try to load sqlite-vec extension for accelerated vector search.
  internal func loadExtensionIfAvailable(extensionPath: String? = nil) {
    guard let db else { return }
    sqlite3_enable_load_extension(db, 1)

    var paths: [String] = []
    if let explicit = extensionPath {
      paths.append(explicit)
    }

    // Try common locations
    let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
    if let appSupport {
      paths.append(appSupport.appendingPathComponent("Peel/vec0.dylib").path)
    }
    paths.append("/usr/local/lib/vec0.dylib")
    paths.append("/opt/homebrew/lib/vec0.dylib")

    for path in paths {
      guard FileManager.default.fileExists(atPath: path) else { continue }
      var errMsg: UnsafeMutablePointer<CChar>?
      let rc = sqlite3_load_extension(db, path, nil, &errMsg)
      if rc == SQLITE_OK {
        extensionLoaded = true
        print("[RAG] sqlite-vec extension loaded from \(path)")
        return
      }
      if let err = errMsg {
        print("[RAG] Failed to load extension from \(path): \(String(cString: err))")
        sqlite3_free(err)
      }
    }
  }

  // MARK: - Schema

  internal func ensureSchema() throws {
    guard db != nil else { throw RAGError.sqlite("Database not initialized") }

    // Create rag_meta table if needed
    try exec("""
      CREATE TABLE IF NOT EXISTS rag_meta (
        key TEXT PRIMARY KEY,
        value TEXT
      )
      """)

    // Read current schema version
    schemaVersion = (try? queryInt("SELECT CAST(value AS INTEGER) FROM rag_meta WHERE key = 'schema_version'")) ?? 0

    if schemaVersion < 1 {
      try exec("""
        CREATE TABLE IF NOT EXISTS repos (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          root_path TEXT NOT NULL,
          last_indexed_at TEXT
        )
        """)
      try exec("""
        CREATE TABLE IF NOT EXISTS files (
          id TEXT PRIMARY KEY,
          repo_id TEXT NOT NULL,
          path TEXT NOT NULL,
          hash TEXT NOT NULL,
          language TEXT,
          updated_at TEXT,
          FOREIGN KEY (repo_id) REFERENCES repos(id)
        )
        """)
      try exec("""
        CREATE TABLE IF NOT EXISTS chunks (
          id TEXT PRIMARY KEY,
          file_id TEXT NOT NULL,
          start_line INTEGER NOT NULL,
          end_line INTEGER NOT NULL,
          text TEXT NOT NULL,
          token_count INTEGER NOT NULL,
          construct_type TEXT,
          construct_name TEXT,
          metadata TEXT,
          FOREIGN KEY (file_id) REFERENCES files(id)
        )
        """)
      try exec("""
        CREATE TABLE IF NOT EXISTS embeddings (
          chunk_id TEXT PRIMARY KEY,
          embedding BLOB NOT NULL,
          FOREIGN KEY (chunk_id) REFERENCES chunks(id)
        )
        """)
      try exec("""
        CREATE TABLE IF NOT EXISTS cache_embeddings (
          text_hash TEXT PRIMARY KEY,
          embedding BLOB NOT NULL,
          updated_at TEXT
        )
        """)
      try setSchemaVersion(1)
    }

    if schemaVersion < 2 {
      try exec("CREATE INDEX IF NOT EXISTS idx_files_repo ON files(repo_id)")
      try exec("CREATE INDEX IF NOT EXISTS idx_chunks_file ON chunks(file_id)")
      try exec("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash)")
      try setSchemaVersion(2)
    }

    if schemaVersion < 3 {
      try exec("""
        CREATE TABLE IF NOT EXISTS rag_query_hints (
          id TEXT PRIMARY KEY,
          query TEXT NOT NULL,
          result_count INTEGER NOT NULL DEFAULT 0,
          search_mode TEXT NOT NULL DEFAULT 'vector',
          created_at TEXT NOT NULL
        )
        """)
      try setSchemaVersion(3)
    }

    if schemaVersion < 4 {
      try exec("""
        CREATE TABLE IF NOT EXISTS symbols (
          id TEXT PRIMARY KEY,
          repo_id TEXT NOT NULL,
          file_id TEXT NOT NULL,
          name TEXT NOT NULL,
          kind TEXT NOT NULL,
          start_line INTEGER,
          end_line INTEGER,
          FOREIGN KEY (repo_id) REFERENCES repos(id),
          FOREIGN KEY (file_id) REFERENCES files(id)
        )
        """)
      try exec("CREATE INDEX IF NOT EXISTS idx_symbols_name ON symbols(name)")
      try exec("CREATE INDEX IF NOT EXISTS idx_symbols_file ON symbols(file_id)")
      try exec("CREATE INDEX IF NOT EXISTS idx_symbols_repo ON symbols(repo_id)")
      try setSchemaVersion(4)
    }

    if schemaVersion < 5 {
      try exec("""
        CREATE TABLE IF NOT EXISTS dependencies (
          id TEXT PRIMARY KEY,
          repo_id TEXT NOT NULL,
          source_file_id TEXT NOT NULL,
          target_path TEXT NOT NULL,
          target_file_id TEXT,
          dependency_type TEXT NOT NULL DEFAULT 'import',
          raw_import TEXT,
          FOREIGN KEY (repo_id) REFERENCES repos(id),
          FOREIGN KEY (source_file_id) REFERENCES files(id),
          FOREIGN KEY (target_file_id) REFERENCES files(id)
        )
        """)
      try exec("CREATE INDEX IF NOT EXISTS idx_deps_source ON dependencies(source_file_id)")
      try exec("CREATE INDEX IF NOT EXISTS idx_deps_target ON dependencies(target_file_id)")
      try exec("CREATE INDEX IF NOT EXISTS idx_deps_repo ON dependencies(repo_id)")
      try setSchemaVersion(5)
    }

    if schemaVersion < 6 {
      if !columnExists("chunks", column: "ai_summary") {
        try exec("ALTER TABLE chunks ADD COLUMN ai_summary TEXT")
      }
      if !columnExists("chunks", column: "ai_tags") {
        try exec("ALTER TABLE chunks ADD COLUMN ai_tags TEXT")
      }
      if !columnExists("chunks", column: "analyzed_at") {
        try exec("ALTER TABLE chunks ADD COLUMN analyzed_at TEXT")
      }
      if !columnExists("chunks", column: "analyzer_model") {
        try exec("ALTER TABLE chunks ADD COLUMN analyzer_model TEXT")
      }
      try setSchemaVersion(6)
    }

    if schemaVersion < 7 {
      if !columnExists("files", column: "module_path") {
        try exec("ALTER TABLE files ADD COLUMN module_path TEXT")
      }
      if !columnExists("files", column: "feature_tags") {
        try exec("ALTER TABLE files ADD COLUMN feature_tags TEXT")
      }
      try setSchemaVersion(7)
    }

    if schemaVersion < 8 {
      if !columnExists("chunks", column: "enriched_at") {
        try exec("ALTER TABLE chunks ADD COLUMN enriched_at TEXT")
      }
      try setSchemaVersion(8)
    }

    if schemaVersion < 9 {
      try exec("""
        CREATE TABLE IF NOT EXISTS lessons (
          id TEXT PRIMARY KEY,
          repo_id TEXT NOT NULL,
          error_signature TEXT,
          file_pattern TEXT,
          fix_description TEXT NOT NULL,
          fix_code TEXT,
          source TEXT NOT NULL DEFAULT 'manual',
          confidence REAL NOT NULL DEFAULT 1.0,
          is_active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          apply_count INTEGER NOT NULL DEFAULT 0,
          success_count INTEGER NOT NULL DEFAULT 0,
          FOREIGN KEY (repo_id) REFERENCES repos(id)
        )
        """)
      try exec("CREATE INDEX IF NOT EXISTS idx_lessons_repo ON lessons(repo_id)")
      try exec("CREATE INDEX IF NOT EXISTS idx_lessons_signature ON lessons(error_signature)")
      try setSchemaVersion(9)
    }

    if schemaVersion < 10 {
      try exec("""
        CREATE TABLE IF NOT EXISTS symbol_refs (
          id TEXT PRIMARY KEY,
          repo_id TEXT NOT NULL,
          source_file_id TEXT NOT NULL,
          referenced_name TEXT NOT NULL,
          ref_kind TEXT NOT NULL DEFAULT 'type',
          FOREIGN KEY (repo_id) REFERENCES repos(id),
          FOREIGN KEY (source_file_id) REFERENCES files(id)
        )
        """)
      try exec("CREATE INDEX IF NOT EXISTS idx_symrefs_source ON symbol_refs(source_file_id)")
      try exec("CREATE INDEX IF NOT EXISTS idx_symrefs_name ON symbol_refs(referenced_name)")
      try exec("CREATE INDEX IF NOT EXISTS idx_symrefs_repo ON symbol_refs(repo_id)")
      try setSchemaVersion(10)
    }

    if schemaVersion < 11 {
      if !columnExists("repos", column: "repo_identifier") {
        try exec("ALTER TABLE repos ADD COLUMN repo_identifier TEXT")
      }
      backfillRepoIdentifiersSync()
      try setSchemaVersion(11)
    }

    if schemaVersion < 12 {
      if !columnExists("repos", column: "parent_repo_id") {
        try exec("ALTER TABLE repos ADD COLUMN parent_repo_id TEXT")
      }
      try setSchemaVersion(12)
    }

    if schemaVersion < 13 {
      try exec("""
        CREATE TABLE IF NOT EXISTS ai_summary_cache (
          text_hash TEXT PRIMARY KEY,
          ai_summary TEXT NOT NULL,
          ai_tags TEXT,
          analyzer_model TEXT,
          cached_at TEXT NOT NULL
        )
        """)
      if !columnExists("files", column: "line_count") {
        try exec("ALTER TABLE files ADD COLUMN line_count INTEGER DEFAULT 0")
      }
      if !columnExists("files", column: "method_count") {
        try exec("ALTER TABLE files ADD COLUMN method_count INTEGER DEFAULT 0")
      }
      if !columnExists("files", column: "byte_size") {
        try exec("ALTER TABLE files ADD COLUMN byte_size INTEGER DEFAULT 0")
      }
      try setSchemaVersion(13)
    }

    if schemaVersion < 14 {
      if !columnExists("repos", column: "embedding_model") {
        try exec("ALTER TABLE repos ADD COLUMN embedding_model TEXT")
      }
      if !columnExists("repos", column: "embedding_dimensions") {
        try exec("ALTER TABLE repos ADD COLUMN embedding_dimensions INTEGER")
      }
      backfillRepoEmbeddingDimensionsSync()
      try setSchemaVersion(14)
    }

    if schemaVersion < 15 || !isCanonicalRagQueryHintsSchema() {
      // Repair legacy rag_query_hints shapes into canonical schema used by
      // recordQueryHint/fetchQueryHints:
      //   id, query, result_count, search_mode, created_at
      let hintColumns = tableColumns("rag_query_hints")
      let requiredColumns: Set<String> = ["id", "query", "result_count", "search_mode", "created_at"]

      if !hintColumns.isEmpty && !requiredColumns.isSubset(of: hintColumns) {
        try exec("DROP TABLE IF EXISTS rag_query_hints_legacy_v15")
        try exec("ALTER TABLE rag_query_hints RENAME TO rag_query_hints_legacy_v15")

        try exec("""
          CREATE TABLE rag_query_hints (
            id TEXT PRIMARY KEY,
            query TEXT NOT NULL,
            result_count INTEGER NOT NULL DEFAULT 0,
            search_mode TEXT NOT NULL DEFAULT 'vector',
            created_at TEXT NOT NULL
          )
          """)

        let legacyColumns = tableColumns("rag_query_hints_legacy_v15")
        let idExpr = legacyColumns.contains("id") ? "id" : "lower(hex(randomblob(16)))"
        let resultCountExpr = legacyColumns.contains("result_count") ? "result_count" : "0"
        let searchModeExpr: String
        if legacyColumns.contains("search_mode") {
          searchModeExpr = "search_mode"
        } else if legacyColumns.contains("mode") {
          searchModeExpr = "mode"
        } else {
          searchModeExpr = "'vector'"
        }
        let createdAtExpr: String
        if legacyColumns.contains("created_at") {
          createdAtExpr = "created_at"
        } else if legacyColumns.contains("last_used_at") {
          createdAtExpr = "last_used_at"
        } else {
          createdAtExpr = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"
        }

        try exec("""
          INSERT INTO rag_query_hints (id, query, result_count, search_mode, created_at)
          SELECT
            COALESCE(NULLIF(
              \(idExpr),
              ''
            ), lower(hex(randomblob(16)))),
            query,
            COALESCE(\(resultCountExpr), 0),
            COALESCE(NULLIF(\(searchModeExpr), ''), 'vector'),
            COALESCE(NULLIF(\(createdAtExpr), ''), strftime('%Y-%m-%dT%H:%M:%fZ','now'))
          FROM rag_query_hints_legacy_v15
          WHERE query IS NOT NULL
          """)

        try exec("DROP TABLE rag_query_hints_legacy_v15")
      } else {
        try exec("""
          CREATE TABLE IF NOT EXISTS rag_query_hints (
            id TEXT PRIMARY KEY,
            query TEXT NOT NULL,
            result_count INTEGER NOT NULL DEFAULT 0,
            search_mode TEXT NOT NULL DEFAULT 'vector',
            created_at TEXT NOT NULL
          )
          """)
        if !columnExists("rag_query_hints", column: "id") {
          try exec("ALTER TABLE rag_query_hints ADD COLUMN id TEXT")
        }
        if !columnExists("rag_query_hints", column: "search_mode") {
          try exec("ALTER TABLE rag_query_hints ADD COLUMN search_mode TEXT NOT NULL DEFAULT 'vector'")
        }
        if !columnExists("rag_query_hints", column: "created_at") {
          try exec("ALTER TABLE rag_query_hints ADD COLUMN created_at TEXT")
        }
      }

      try exec("""
        UPDATE rag_query_hints
        SET
          id = COALESCE(NULLIF(id, ''), lower(hex(randomblob(16)))),
          search_mode = COALESCE(NULLIF(search_mode, ''), 'vector'),
          created_at = COALESCE(NULLIF(created_at, ''), strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        """)

      try exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_rag_query_hints_id ON rag_query_hints(id)")
      try exec("CREATE INDEX IF NOT EXISTS idx_rag_query_hints_created_at ON rag_query_hints(created_at DESC)")

      try setSchemaVersion(15)
    }

    if schemaVersion < 16 {
      backfillRepoEmbeddingModelSync()
      try setSchemaVersion(16)
    }

    // Set up vec table if extension is loaded
    if extensionLoaded {
      try ensureVecTable()
    }
  }

  internal func ensureVecTable() throws {
    guard extensionLoaded else { return }
    let dims = embeddingProvider.dimensions
    try exec("""
      CREATE VIRTUAL TABLE IF NOT EXISTS vec_chunks USING vec0 (
        chunk_id TEXT PRIMARY KEY,
        embedding float[\(dims)]
      )
      """)
  }

  /// Sync existing embeddings into the vec_chunks table (initial population).
  public func syncVecTable() throws {
    guard extensionLoaded else { return }
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    let count = try queryInt("SELECT COUNT(*) FROM embeddings")
    let vecCount = try queryInt("SELECT COUNT(*) FROM vec_chunks")
    guard vecCount == 0 && count > 0 else { return }

    print("[RAG] Syncing \(count) embeddings to vec_chunks")
    try exec("BEGIN TRANSACTION")
    do {
      let sql = "SELECT chunk_id, embedding FROM embeddings"
      var stmt: OpaquePointer?
      let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
      guard result == SQLITE_OK, let stmt else {
        throw RAGError.sqlite("Failed to prepare sync query")
      }
      defer { sqlite3_finalize(stmt) }

      var synced = 0
      while sqlite3_step(stmt) == SQLITE_ROW {
        let chunkId = String(cString: sqlite3_column_text(stmt, 0))
        guard let blob = sqlite3_column_blob(stmt, 1) else { continue }
        let blobSize = sqlite3_column_bytes(stmt, 1)

        let insertSql = "INSERT OR IGNORE INTO vec_chunks (chunk_id, embedding) VALUES (?, ?)"
        var insertStmt: OpaquePointer?
        sqlite3_prepare_v2(db, insertSql, -1, &insertStmt, nil)
        guard let insertStmt else { continue }
        defer { sqlite3_finalize(insertStmt) }

        sqlite3_bind_text(insertStmt, 1, chunkId, -1, sqliteTransient)
        sqlite3_bind_blob(insertStmt, 2, blob, blobSize, sqliteTransient)
        sqlite3_step(insertStmt)
        synced += 1
      }
      try exec("COMMIT")
      print("[RAG] Synced \(synced) embeddings to vec_chunks")
    } catch {
      try? exec("ROLLBACK")
      throw error
    }
  }

  /// Backfill repo_identifier for existing repos.
  internal func backfillRepoIdentifiersSync() {
    guard let db else { return }
    let sql = "SELECT id, root_path FROM repos WHERE repo_identifier IS NULL"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return }
    defer { sqlite3_finalize(stmt) }

    var updates: [(id: String, identifier: String)] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let id = String(cString: sqlite3_column_text(stmt, 0))
      let rootPath = String(cString: sqlite3_column_text(stmt, 1))
      if let identifier = Self.discoverNormalizedRemoteURL(for: rootPath) {
        updates.append((id, identifier))
      }
    }

    for update in updates {
      let updateSql = "UPDATE repos SET repo_identifier = ? WHERE id = ?"
      var updateStmt: OpaquePointer?
      guard sqlite3_prepare_v2(db, updateSql, -1, &updateStmt, nil) == SQLITE_OK, let updateStmt else { continue }
      defer { sqlite3_finalize(updateStmt) }
      sqlite3_bind_text(updateStmt, 1, update.identifier, -1, sqliteTransient)
      sqlite3_bind_text(updateStmt, 2, update.id, -1, sqliteTransient)
      sqlite3_step(updateStmt)
    }

    if !updates.isEmpty {
      print("[RAG] Backfilled repo_identifier for \(updates.count) repos")
    }
  }

  /// Backfill embedding dimensions for repos by sampling stored embedding blobs.
  internal func backfillRepoEmbeddingDimensionsSync() {
    guard let db else { return }
    let sql = """
      SELECT f.repo_id, CAST(LENGTH(e.embedding) / 4 AS INTEGER) AS dims
      FROM embeddings e
      JOIN chunks c ON c.id = e.chunk_id
      JOIN files f ON f.id = c.file_id
      GROUP BY f.repo_id
      """
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return }
    defer { sqlite3_finalize(stmt) }

    var updates: [(repoId: String, dims: Int)] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let repoId = String(cString: sqlite3_column_text(stmt, 0))
      let dims = Int(sqlite3_column_int(stmt, 1))
      updates.append((repoId: repoId, dims: dims))
    }

    for update in updates {
      let updateSql = "UPDATE repos SET embedding_dimensions = COALESCE(embedding_dimensions, ?) WHERE id = ?"
      var updateStmt: OpaquePointer?
      guard sqlite3_prepare_v2(db, updateSql, -1, &updateStmt, nil) == SQLITE_OK, let updateStmt else { continue }
      defer { sqlite3_finalize(updateStmt) }
      sqlite3_bind_int(updateStmt, 1, Int32(update.dims))
      sqlite3_bind_text(updateStmt, 2, update.repoId, -1, sqliteTransient)
      sqlite3_step(updateStmt)
    }

    if !updates.isEmpty {
      print("[RAG] Backfilled embedding dimensions for \(updates.count) repos")
    }
  }

  /// Backfill embedding_model for repos that have dimensions but no model name.
  /// Maps known MLX tier dimensions to their canonical model names.
  internal func backfillRepoEmbeddingModelSync() {
    guard let db else { return }

    // Known dimension → model mappings for MLX embedding tiers
    let dimensionToModel: [(dims: Int, model: String)] = [
      (384, "all-MiniLM-L6-v2"),
      (768, "nomic-embed-text-v1.5"),
      (1024, "Qwen3-Embedding-0.6B-4bit"),
    ]

    var totalUpdated = 0
    for mapping in dimensionToModel {
      let sql = """
        UPDATE repos
        SET embedding_model = ?
        WHERE embedding_model IS NULL
          AND embedding_dimensions = ?
        """
      var stmt: OpaquePointer?
      guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { continue }
      defer { sqlite3_finalize(stmt) }
      sqlite3_bind_text(stmt, 1, mapping.model, -1, sqliteTransient)
      sqlite3_bind_int(stmt, 2, Int32(mapping.dims))
      sqlite3_step(stmt)
      totalUpdated += Int(sqlite3_changes(db))
    }

    if totalUpdated > 0 {
      print("[RAG] Backfilled embedding_model for \(totalUpdated) repos from dimensions")
    }
  }

  // MARK: - Schema Helpers

  internal func columnExists(_ table: String, column: String) -> Bool {
    guard let db else { return false }
    let sql = "PRAGMA table_info(\(table))"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return false }
    defer { sqlite3_finalize(stmt) }
    while sqlite3_step(stmt) == SQLITE_ROW {
      if let name = sqlite3_column_text(stmt, 1) {
        if String(cString: name) == column { return true }
      }
    }
    return false
  }

  internal func tableColumns(_ table: String) -> Set<String> {
    guard let db else { return [] }
    let sql = "PRAGMA table_info(\(table))"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return [] }
    defer { sqlite3_finalize(stmt) }

    var columns: Set<String> = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      if let name = sqlite3_column_text(stmt, 1) {
        columns.insert(String(cString: name))
      }
    }
    return columns
  }

  internal func isCanonicalRagQueryHintsSchema() -> Bool {
    let columns = tableColumns("rag_query_hints")
    let required: Set<String> = ["id", "query", "result_count", "search_mode", "created_at"]
    return !columns.isEmpty && required.isSubset(of: columns)
  }

  private func setSchemaVersion(_ version: Int) throws {
    try exec("INSERT OR REPLACE INTO rag_meta (key, value) VALUES ('schema_version', '\(version)')")
    schemaVersion = version
  }
}
