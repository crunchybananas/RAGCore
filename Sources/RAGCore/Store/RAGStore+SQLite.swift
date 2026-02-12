//
//  RAGStore+SQLite.swift
//  RAGCore
//
//  Low-level SQLite helper methods for the RAGStore actor.
//

import CSQLite
import Foundation

extension RAGStore {

  // MARK: - SQL Execution Helpers

  internal func exec(_ sql: String) throws {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var errMsg: UnsafeMutablePointer<CChar>?
    let result = sqlite3_exec(db, sql, nil, nil, &errMsg)
    guard result == SQLITE_OK else {
      let message = errMsg.map { String(cString: $0) } ?? "Unknown error"
      sqlite3_free(errMsg)
      throw RAGError.sqlite(message)
    }
  }

  internal func queryString(_ sql: String) throws -> String? {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { return nil }
    defer { sqlite3_finalize(stmt) }
    guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
    guard let text = sqlite3_column_text(stmt, 0) else { return nil }
    return String(cString: text)
  }

  internal func queryInt(_ sql: String) throws -> Int {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { return 0 }
    defer { sqlite3_finalize(stmt) }
    guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
    return Int(sqlite3_column_int(stmt, 0))
  }

  internal func queryInt(_ sql: String, bind: (OpaquePointer) -> Void) throws -> Int {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { return 0 }
    defer { sqlite3_finalize(stmt) }
    bind(stmt)
    guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
    return Int(sqlite3_column_int(stmt, 0))
  }

  internal func queryRow(_ sql: String) throws -> (String, String)? {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { return nil }
    defer { sqlite3_finalize(stmt) }
    guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
    guard let col0 = sqlite3_column_text(stmt, 0),
          let col1 = sqlite3_column_text(stmt, 1) else { return nil }
    return (String(cString: col0), String(cString: col1))
  }

  internal func execute(sql: String, binder: (OpaquePointer) -> Void) throws {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK else {
      let message = String(cString: sqlite3_errmsg(db))
      throw RAGError.sqlite(message)
    }
    guard let statement else { throw RAGError.sqlite("Failed to prepare statement") }
    defer { sqlite3_finalize(statement) }
    binder(statement)
    let stepResult = sqlite3_step(statement)
    guard stepResult == SQLITE_DONE else {
      let message = String(cString: sqlite3_errmsg(db))
      throw RAGError.sqlite(message)
    }
  }

  internal func bindText(_ statement: OpaquePointer, _ index: Int32, _ value: String) {
    let cString = (value as NSString).utf8String
    sqlite3_bind_text(statement, index, cString, -1, sqliteTransient)
  }

  internal func bindTextOrNull(_ statement: OpaquePointer, _ index: Int32, _ value: String?) {
    if let value {
      bindText(statement, index, value)
    } else {
      sqlite3_bind_null(statement, index)
    }
  }

  // MARK: - Embedding Row Queries

  /// Row type returned from embedding queries.
  struct EmbeddingRow {
    let chunkId: String
    let embedding: [Float]
    let filePath: String
    let startLine: Int
    let endLine: Int
    let snippet: String
    let constructType: String?
    let constructName: String?
    let language: String?
    let isTest: Bool
    let modulePath: String?
    let featureTags: String?
    let tokenCount: Int
  }

  internal func queryEmbeddingRows(
    sql: String,
    binder: (OpaquePointer) -> Void
  ) throws -> [EmbeddingRow] {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }
    binder(stmt)

    var rows: [EmbeddingRow] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      guard let blob = sqlite3_column_blob(stmt, 0) else { continue }
      let blobSize = sqlite3_column_bytes(stmt, 0)
      let floatCount = Int(blobSize) / MemoryLayout<Float>.size
      let embedding = Array(UnsafeBufferPointer(
        start: blob.assumingMemoryBound(to: Float.self),
        count: floatCount
      ))

      let chunkId = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
      let filePath = sqlite3_column_text(stmt, 2).map { String(cString: $0) } ?? ""
      let startLine = Int(sqlite3_column_int(stmt, 3))
      let endLine = Int(sqlite3_column_int(stmt, 4))
      let snippet = sqlite3_column_text(stmt, 5).map { String(cString: $0) } ?? ""
      let constructType = sqlite3_column_text(stmt, 6).map { String(cString: $0) }
      let constructName = sqlite3_column_text(stmt, 7).map { String(cString: $0) }
      let language = sqlite3_column_text(stmt, 8).map { String(cString: $0) }
      let modulePath = sqlite3_column_text(stmt, 9).map { String(cString: $0) }
      let featureTags = sqlite3_column_text(stmt, 10).map { String(cString: $0) }
      let aiSummary = sqlite3_column_text(stmt, 11).map { String(cString: $0) }
      let tokenCount = Int(sqlite3_column_int(stmt, 12))
      _ = aiSummary // used by caller via querySearchResults

      rows.append(EmbeddingRow(
        chunkId: chunkId,
        embedding: embedding,
        filePath: filePath,
        startLine: startLine,
        endLine: endLine,
        snippet: snippet,
        constructType: constructType,
        constructName: constructName,
        language: language,
        isTest: isTestFile(filePath),
        modulePath: modulePath,
        featureTags: featureTags,
        tokenCount: tokenCount
      ))
    }
    return rows
  }

  // MARK: - Search Result Queries

  /// Query and map search results from a SQL statement.
  internal func querySearchResults(
    sql: String,
    withScore: Bool,
    binder: (OpaquePointer) -> Void
  ) throws -> [RAGSearchResult] {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else {
      let message = String(cString: sqlite3_errmsg(db))
      throw RAGError.sqlite(message)
    }
    defer { sqlite3_finalize(stmt) }
    binder(stmt)

    var results: [RAGSearchResult] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let filePath = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let startLine = Int(sqlite3_column_int(stmt, 1))
      let endLine = Int(sqlite3_column_int(stmt, 2))
      let snippet = sqlite3_column_text(stmt, 3).map { String(cString: $0) } ?? ""
      let constructType = sqlite3_column_text(stmt, 4).map { String(cString: $0) }
      let constructName = sqlite3_column_text(stmt, 5).map { String(cString: $0) }
      let language = sqlite3_column_text(stmt, 6).map { String(cString: $0) }
      let modulePath = sqlite3_column_text(stmt, 7).map { String(cString: $0) }
      let featureTagsJson = sqlite3_column_text(stmt, 8).map { String(cString: $0) }
      let aiSummary = sqlite3_column_text(stmt, 9).map { String(cString: $0) }
      let aiTagsJson = sqlite3_column_text(stmt, 10).map { String(cString: $0) }
      let tokenCount = Int(sqlite3_column_int(stmt, 11))

      let featureTags: [String]?
      if let json = featureTagsJson, let data = json.data(using: .utf8) {
        featureTags = try? JSONDecoder().decode([String].self, from: data)
      } else {
        featureTags = nil
      }

      let aiTags: [String]?
      if let json = aiTagsJson, let data = json.data(using: .utf8) {
        aiTags = try? JSONDecoder().decode([String].self, from: data)
      } else {
        aiTags = nil
      }

      results.append(RAGSearchResult(
        filePath: filePath,
        startLine: startLine,
        endLine: endLine,
        snippet: snippet,
        constructType: constructType,
        constructName: constructName,
        language: language,
        isTest: isTestFile(filePath),
        score: withScore ? 0.0 : nil,
        modulePath: modulePath,
        featureTags: featureTags ?? [],
        aiSummary: aiSummary,
        aiTags: aiTags ?? [],
        tokenCount: tokenCount
      ))
    }
    return results
  }

  // MARK: - CRUD Operations

  internal func upsertRepo(
    id: String,
    name: String,
    rootPath: String,
    lastIndexedAt: String?,
    repoIdentifier: String? = nil,
    parentRepoId: String? = nil
  ) throws {
    let sql = """
      INSERT INTO repos (id, name, root_path, last_indexed_at, repo_identifier, parent_repo_id)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        name = excluded.name,
        root_path = excluded.root_path,
        last_indexed_at = excluded.last_indexed_at,
        repo_identifier = COALESCE(excluded.repo_identifier, repos.repo_identifier),
        parent_repo_id = excluded.parent_repo_id
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, id)
      bindText(stmt, 2, name)
      bindText(stmt, 3, rootPath)
      bindTextOrNull(stmt, 4, lastIndexedAt)
      bindTextOrNull(stmt, 5, repoIdentifier)
      bindTextOrNull(stmt, 6, parentRepoId)
    }
  }

  internal func upsertFile(
    id: String,
    repoId: String,
    path: String,
    hash: String,
    language: String,
    updatedAt: String,
    modulePath: String?,
    featureTags: String?,
    lineCount: Int = 0,
    methodCount: Int = 0,
    byteSize: Int = 0
  ) throws {
    let sql = """
      INSERT INTO files (id, repo_id, path, hash, language, updated_at, module_path, feature_tags, line_count, method_count, byte_size)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        repo_id = excluded.repo_id,
        path = excluded.path,
        hash = excluded.hash,
        language = excluded.language,
        updated_at = excluded.updated_at,
        module_path = excluded.module_path,
        feature_tags = excluded.feature_tags,
        line_count = excluded.line_count,
        method_count = excluded.method_count,
        byte_size = excluded.byte_size
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, id)
      bindText(stmt, 2, repoId)
      bindText(stmt, 3, path)
      bindText(stmt, 4, hash)
      bindText(stmt, 5, language)
      bindText(stmt, 6, updatedAt)
      bindTextOrNull(stmt, 7, modulePath)
      bindTextOrNull(stmt, 8, featureTags)
      sqlite3_bind_int(stmt, 9, Int32(lineCount))
      sqlite3_bind_int(stmt, 10, Int32(methodCount))
      sqlite3_bind_int(stmt, 11, Int32(byteSize))
    }
  }

  internal func upsertChunk(
    id: String,
    fileId: String,
    startLine: Int,
    endLine: Int,
    text: String,
    tokenCount: Int,
    constructType: String?,
    constructName: String?,
    metadata: String?,
    aiSummary: String? = nil,
    aiTags: String? = nil,
    analyzedAt: String? = nil,
    analyzerModel: String? = nil
  ) throws {
    let sql = """
      INSERT OR REPLACE INTO chunks (id, file_id, start_line, end_line, text, token_count, construct_type, construct_name, metadata, ai_summary, ai_tags, analyzed_at, analyzer_model)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, id)
      bindText(stmt, 2, fileId)
      sqlite3_bind_int(stmt, 3, Int32(startLine))
      sqlite3_bind_int(stmt, 4, Int32(endLine))
      bindText(stmt, 5, text)
      sqlite3_bind_int(stmt, 6, Int32(tokenCount))
      bindTextOrNull(stmt, 7, constructType)
      bindTextOrNull(stmt, 8, constructName)
      bindTextOrNull(stmt, 9, metadata)
      bindTextOrNull(stmt, 10, aiSummary)
      bindTextOrNull(stmt, 11, aiTags)
      bindTextOrNull(stmt, 12, analyzedAt)
      bindTextOrNull(stmt, 13, analyzerModel)
    }
  }

  internal func deleteChunks(for fileId: String) throws {
    if extensionLoaded {
      let vecSql = "DELETE FROM vec_chunks WHERE chunk_id IN (SELECT id FROM chunks WHERE file_id = ?)"
      try execute(sql: vecSql) { stmt in bindText(stmt, 1, fileId) }
    }
    try execute(sql: "DELETE FROM chunks WHERE file_id = ?") { stmt in
      bindText(stmt, 1, fileId)
    }
  }

  internal func upsertEmbedding(chunkId: String, vector: [Float]) throws {
    let sql = "INSERT OR REPLACE INTO embeddings (chunk_id, embedding) VALUES (?, ?)"
    let data = VectorMath.encodeVector(vector)
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, chunkId)
      _ = data.withUnsafeBytes { bytes in
        sqlite3_bind_blob(stmt, 2, bytes.baseAddress, Int32(data.count), sqliteTransient)
      }
    }
    if extensionLoaded {
      let deleteSql = "DELETE FROM vec_chunks WHERE chunk_id = ?"
      try execute(sql: deleteSql) { stmt in bindText(stmt, 1, chunkId) }
      let vecSql = "INSERT INTO vec_chunks (chunk_id, embedding) VALUES (?, ?)"
      try execute(sql: vecSql) { stmt in
        bindText(stmt, 1, chunkId)
        _ = data.withUnsafeBytes { bytes in
          sqlite3_bind_blob(stmt, 2, bytes.baseAddress, Int32(data.count), sqliteTransient)
        }
      }
    }
  }

  internal func fetchCachedEmbedding(textHash: String) throws -> [Float]? {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = "SELECT embedding FROM cache_embeddings WHERE text_hash = ? LIMIT 1"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return nil }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, textHash)
    guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
    guard let blob = sqlite3_column_blob(stmt, 0) else { return nil }
    let size = sqlite3_column_bytes(stmt, 0)
    guard size > 0 else { return nil }
    return VectorMath.decodeVector(Data(bytes: blob, count: Int(size)))
  }

  internal func upsertCacheEmbedding(textHash: String, vector: [Float]) throws {
    let sql = "INSERT OR REPLACE INTO cache_embeddings (text_hash, embedding, updated_at) VALUES (?, ?, ?)"
    let data = VectorMath.encodeVector(vector)
    let now = dateFormatter.string(from: Date())
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, textHash)
      _ = data.withUnsafeBytes { bytes in
        sqlite3_bind_blob(stmt, 2, bytes.baseAddress, Int32(data.count), sqliteTransient)
      }
      bindText(stmt, 3, now)
    }
  }

  internal func fetchFileHash(repoId: String, fileId: String) throws -> String? {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = "SELECT hash FROM files WHERE id = ? AND repo_id = ?"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return nil }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, fileId)
    bindText(stmt, 2, repoId)
    guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
    return sqlite3_column_text(stmt, 0).map { String(cString: $0) }
  }

  internal func fetchFileHashByPath(repoId: String, path: String) throws -> String? {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = "SELECT hash FROM files WHERE path = ? AND repo_id = ?"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return nil }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, path)
    bindText(stmt, 2, repoId)
    guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
    return sqlite3_column_text(stmt, 0).map { String(cString: $0) }
  }

  // MARK: - AI Summary Cache

  internal func cacheAIAnalysis(for fileId: String) throws {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = """
      SELECT text, ai_summary, ai_tags, analyzer_model
      FROM chunks WHERE file_id = ? AND ai_summary IS NOT NULL
      """
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, fileId)

    var toCache: [(textHash: String, summary: String, tags: String, model: String)] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      guard let textPtr = sqlite3_column_text(stmt, 0),
            let summaryPtr = sqlite3_column_text(stmt, 1) else { continue }
      let text = String(cString: textPtr)
      let summary = String(cString: summaryPtr)
      let tags = sqlite3_column_text(stmt, 2).map { String(cString: $0) } ?? "[]"
      let model = sqlite3_column_text(stmt, 3).map { String(cString: $0) } ?? "unknown"
      toCache.append((VectorMath.stableId(for: text), summary, tags, model))
    }

    let now = dateFormatter.string(from: Date())
    for item in toCache {
      try upsertAIAnalysisCache(textHash: item.textHash, summary: item.summary, tags: item.tags, model: item.model, cachedAt: now)
    }
  }

  internal func fetchCachedAIAnalysis(textHash: String) throws -> CachedAIAnalysis? {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = "SELECT ai_summary, ai_tags, analyzer_model FROM ai_summary_cache WHERE text_hash = ? LIMIT 1"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return nil }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, textHash)
    guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
    guard let summaryPtr = sqlite3_column_text(stmt, 0) else { return nil }
    let summary = String(cString: summaryPtr)
    let tags = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? "[]"
    let model = sqlite3_column_text(stmt, 2).map { String(cString: $0) } ?? "unknown"
    return CachedAIAnalysis(summary: summary, tags: tags, model: model)
  }

  internal func upsertAIAnalysisCache(
    textHash: String,
    summary: String,
    tags: String,
    model: String,
    cachedAt: String
  ) throws {
    let sql = """
      INSERT OR REPLACE INTO ai_summary_cache (text_hash, ai_summary, ai_tags, analyzer_model, cached_at)
      VALUES (?, ?, ?, ?, ?)
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, textHash)
      bindText(stmt, 2, summary)
      bindText(stmt, 3, tags)
      bindText(stmt, 4, model)
      bindText(stmt, 5, cachedAt)
    }
  }
}
