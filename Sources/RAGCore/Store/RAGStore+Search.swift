//
//  RAGStore+Search.swift
//  RAGCore
//
//  Text (FTS5-like) and vector search methods.
//

import CSQLite
import Foundation

extension RAGStore {

  // MARK: - Text Search

  /// Search by text (keyword matching across code, construct names, and AI summaries).
  ///
  /// - Parameters:
  ///   - query: Search query string.
  ///   - repoPath: Optional repo path to scope the search.
  ///   - limit: Maximum results.
  ///   - matchAll: If true, all words must appear (AND). If false, any word (OR).
  ///   - modulePath: Optional module path filter.
  /// - Returns: Matching search results.
  public func search(
    query: String,
    repoPath: String? = nil,
    limit: Int = 10,
    matchAll: Bool = true,
    modulePath: String? = nil
  ) async throws -> [RAGSearchResult] {
    let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmedQuery.isEmpty else { return [] }
    try openIfNeeded()

    // Resolve repo identity (path-independent)
    let resolvedRepoId: String?
    if let repoPath {
      resolvedRepoId = try resolveRepo(for: repoPath)?.id
      guard resolvedRepoId != nil else { return [] }
    } else {
      resolvedRepoId = nil
    }

    let words = trimmedQuery
      .components(separatedBy: .whitespacesAndNewlines)
      .filter { !$0.isEmpty }

    var whereClauses = [String]()
    for _ in words {
      whereClauses.append("(chunks.text LIKE ? OR chunks.construct_name LIKE ? OR chunks.ai_summary LIKE ?)")
    }

    let joinOp = matchAll ? " AND " : " OR "

    var sqlBase = """
      SELECT repos.root_path || '/' || files.path, chunks.start_line, chunks.end_line, chunks.text,
             chunks.construct_type, chunks.construct_name, files.language, files.module_path, files.feature_tags,
             chunks.ai_summary, chunks.ai_tags, chunks.token_count
      FROM chunks
      JOIN files ON files.id = chunks.file_id
      JOIN repos ON repos.id = files.repo_id
      WHERE (\(whereClauses.joined(separator: joinOp)))
      """

    if modulePath != nil {
      sqlBase += " AND LOWER(files.module_path) LIKE ?"
    }

    let sql: String
    if resolvedRepoId != nil {
      sql = sqlBase + " AND repos.id = ? ORDER BY files.path LIMIT ?"
    } else {
      sql = sqlBase + " ORDER BY files.path LIMIT ?"
    }

    return try querySearchResults(sql: sql, withScore: false) { statement in
      var bindIndex: Int32 = 1
      for word in words {
        let pattern = "%\(word)%"
        bindText(statement, bindIndex, pattern)
        bindText(statement, bindIndex + 1, pattern)
        bindText(statement, bindIndex + 2, pattern)
        bindIndex += 3
      }
      if let modulePath {
        bindText(statement, bindIndex, "%\(modulePath.lowercased())%")
        bindIndex += 1
      }
      if let resolvedRepoId {
        bindText(statement, bindIndex, resolvedRepoId)
        bindIndex += 1
      }
      sqlite3_bind_int(statement, bindIndex, Int32(max(1, limit)))
    }
  }

  // MARK: - Vector Search

  /// Search using vector similarity (semantic search).
  ///
  /// - Parameters:
  ///   - query: Natural language query to search for.
  ///   - repoPath: Optional repo path to scope the search.
  ///   - limit: Maximum number of results.
  ///   - threshold: Minimum similarity score (0.0-1.0).
  ///   - modulePath: Optional module path filter.
  /// - Returns: Search results ordered by relevance.
  public func searchVector(
    query: String,
    repoPath: String? = nil,
    limit: Int = 10,
    threshold: Float = 0.3,
    modulePath: String? = nil
  ) async throws -> [RAGSearchResult] {
    try openIfNeeded()
    try ensureSchema()

    // Resolve repo identity before searching
    let resolvedRepoId: String?
    if let repoPath {
      resolvedRepoId = try resolveRepo(for: repoPath)?.id
      guard resolvedRepoId != nil else { return [] }
    } else {
      resolvedRepoId = nil
    }

    let embeddings = try await embeddingProvider.embed(texts: [query])
    guard let queryVector = embeddings.first, !queryVector.isEmpty else {
      throw RAGError.embeddingFailed("Failed to generate query embedding")
    }

    return try searchVectorWithEmbedding(
      queryVector, resolvedRepoId: resolvedRepoId,
      limit: limit, threshold: threshold, modulePath: modulePath
    )
  }

  /// Search using a pre-computed query embedding vector.
  ///
  /// Use this when the caller has already generated the query embedding
  /// (e.g. using a different model than the store's default provider to
  /// match the stored embedding dimensions for a synced repo).
  ///
  /// - Parameters:
  ///   - queryEmbedding: Pre-computed embedding vector for the query.
  ///   - repoPath: Optional repo path to scope the search.
  ///   - limit: Maximum number of results.
  ///   - threshold: Minimum similarity score (0.0-1.0).
  ///   - modulePath: Optional module path filter.
  /// - Returns: Search results ordered by relevance.
  public func searchVectorWithEmbedding(
    _ queryEmbedding: [Float],
    repoPath: String? = nil,
    limit: Int = 10,
    threshold: Float = 0.3,
    modulePath: String? = nil
  ) throws -> [RAGSearchResult] {
    try openIfNeeded()
    try ensureSchema()
    guard !queryEmbedding.isEmpty else {
      throw RAGError.embeddingFailed("Empty query embedding vector")
    }

    // Resolve repo identity
    let resolvedRepoId: String?
    if let repoPath {
      resolvedRepoId = try resolveRepo(for: repoPath)?.id
      guard resolvedRepoId != nil else { return [] }
    } else {
      resolvedRepoId = nil
    }

    return try searchVectorWithEmbedding(
      queryEmbedding, resolvedRepoId: resolvedRepoId,
      limit: limit, threshold: threshold, modulePath: modulePath
    )
  }

  /// Internal: search with pre-resolved repo ID.
  internal func searchVectorWithEmbedding(
    _ queryEmbedding: [Float],
    resolvedRepoId: String?,
    limit: Int = 10,
    threshold: Float = 0.3,
    modulePath: String? = nil
  ) throws -> [RAGSearchResult] {
    if extensionLoaded {
      return try searchVectorAccelerated(
        queryVector: queryEmbedding, resolvedRepoId: resolvedRepoId,
        limit: limit, threshold: threshold, modulePath: modulePath
      )
    } else {
      return try searchVectorBruteForce(
        queryVector: queryEmbedding, resolvedRepoId: resolvedRepoId,
        limit: limit, threshold: threshold, modulePath: modulePath
      )
    }
  }

  /// Accelerated vector search using sqlite-vec extension.
  internal func searchVectorAccelerated(
    queryVector: [Float],
    resolvedRepoId: String?,
    limit: Int,
    threshold: Float,
    modulePath: String?
  ) throws -> [RAGSearchResult] {
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    // Use vec_distance_cosine for accelerated search
    var sql = """
      SELECT
        repos.root_path || '/' || files.path,
        chunks.start_line, chunks.end_line, chunks.text,
        chunks.construct_type, chunks.construct_name,
        files.language, files.module_path, files.feature_tags,
        chunks.ai_summary, chunks.ai_tags, chunks.token_count,
        vec_distance_cosine(v.embedding, ?) as distance
      FROM vec_chunks v
      JOIN chunks ON chunks.id = v.chunk_id
      JOIN files ON files.id = chunks.file_id
      JOIN repos ON repos.id = files.repo_id
      WHERE 1=1
      """

    if resolvedRepoId != nil { sql += " AND repos.id = ?" }
    if modulePath != nil { sql += " AND LOWER(files.module_path) LIKE ?" }
    sql += " ORDER BY distance ASC LIMIT ?"

    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }

    let vectorData = queryVector.withUnsafeBytes { Data($0) }
    vectorData.withUnsafeBytes { ptr in
      sqlite3_bind_blob(stmt, 1, ptr.baseAddress, Int32(vectorData.count), nil)
    }

    var bindIdx: Int32 = 2
    if let resolvedRepoId { bindText(stmt, bindIdx, resolvedRepoId); bindIdx += 1 }
    if let modulePath { bindText(stmt, bindIdx, "%\(modulePath.lowercased())%"); bindIdx += 1 }
    sqlite3_bind_int(stmt, bindIdx, Int32(limit * 2))

    var results: [RAGSearchResult] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let filePath = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let startLine = Int(sqlite3_column_int(stmt, 1))
      let endLine = Int(sqlite3_column_int(stmt, 2))
      let snippet = sqlite3_column_text(stmt, 3).map { String(cString: $0) } ?? ""
      let constructType = sqlite3_column_text(stmt, 4).map { String(cString: $0) }
      let constructName = sqlite3_column_text(stmt, 5).map { String(cString: $0) }
      let language = sqlite3_column_text(stmt, 6).map { String(cString: $0) }
      let modPath = sqlite3_column_text(stmt, 7).map { String(cString: $0) }
      let featureTagsJson = sqlite3_column_text(stmt, 8).map { String(cString: $0) }
      let aiSummary = sqlite3_column_text(stmt, 9).map { String(cString: $0) }
      let aiTagsJson = sqlite3_column_text(stmt, 10).map { String(cString: $0) }
      let tokenCount = Int(sqlite3_column_int(stmt, 11))
      let distance = Float(sqlite3_column_double(stmt, 12))

      let similarity = max(0, 1.0 - distance)
      guard similarity >= threshold else { continue }

      let featureTags = featureTagsJson.flatMap { json in
        json.data(using: .utf8).flatMap { try? JSONDecoder().decode([String].self, from: $0) }
      }
      let aiTags = aiTagsJson.flatMap { json in
        json.data(using: .utf8).flatMap { try? JSONDecoder().decode([String].self, from: $0) }
      }

      results.append(RAGSearchResult(
        filePath: filePath, startLine: startLine, endLine: endLine,
        snippet: snippet, constructType: constructType, constructName: constructName,
        language: language, isTest: isTestFile(filePath), score: similarity,
        modulePath: modPath, featureTags: featureTags ?? [],
        aiSummary: aiSummary, aiTags: aiTags ?? [], tokenCount: tokenCount
      ))

      if results.count >= limit { break }
    }

    return results
  }

  /// Brute-force vector search using cosine similarity.
  internal func searchVectorBruteForce(
    queryVector: [Float],
    resolvedRepoId: String?,
    limit: Int,
    threshold: Float,
    modulePath: String?
  ) throws -> [RAGSearchResult] {
    var sql = """
      SELECT e.embedding, chunks.id,
             repos.root_path || '/' || files.path,
             chunks.start_line, chunks.end_line, chunks.text,
             chunks.construct_type, chunks.construct_name,
             files.language, files.module_path, files.feature_tags,
             chunks.ai_summary, chunks.token_count
      FROM embeddings e
      JOIN chunks ON chunks.id = e.chunk_id
      JOIN files ON files.id = chunks.file_id
      JOIN repos ON repos.id = files.repo_id
      WHERE 1=1
      """

    if resolvedRepoId != nil { sql += " AND repos.id = ?" }
    if modulePath != nil { sql += " AND LOWER(files.module_path) LIKE ?" }

    let rows = try queryEmbeddingRows(sql: sql) { stmt in
      var idx: Int32 = 1
      if let resolvedRepoId { bindText(stmt, idx, resolvedRepoId); idx += 1 }
      if let modulePath { bindText(stmt, idx, "%\(modulePath.lowercased())%") }
    }

    // Compute similarities and rank
    var scored: [(row: EmbeddingRow, score: Float)] = []
    for row in rows {
      let sim = VectorMath.cosineSimilarity(queryVector, row.embedding)
      guard sim >= threshold else { continue }
      scored.append((row, sim))
    }

    scored.sort { $0.score > $1.score }

    return scored.prefix(limit).map { item in
      let row = item.row
      let featureTags = row.featureTags.flatMap { json in
        json.data(using: .utf8).flatMap { try? JSONDecoder().decode([String].self, from: $0) }
      }
      return RAGSearchResult(
        filePath: row.filePath, startLine: row.startLine, endLine: row.endLine,
        snippet: row.snippet, constructType: row.constructType, constructName: row.constructName,
        language: row.language, isTest: row.isTest, score: item.score,
        modulePath: row.modulePath, featureTags: featureTags ?? [],
        aiSummary: nil, aiTags: [], tokenCount: row.tokenCount
      )
    }
  }
}
