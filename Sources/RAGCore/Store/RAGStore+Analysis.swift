//
//  RAGStore+Analysis.swift
//  RAGCore
//
//  AI chunk analysis, enrichment, and code analytics (duplicates, patterns, hotspots).
//

import CSQLite
import Foundation

extension RAGStore {
  private static let failedAnalysisSummary = "[analysis-failed]"
  private static let failedAnalysisModel = "chunk-analyzer-failed"

  // MARK: - AI Chunk Analysis

  /// Analyze un-analyzed chunks using the configured `ChunkAnalyzer`.
  /// Returns count of chunks analyzed.
  public func analyzeChunks(
    repoPath: String? = nil,
    limit: Int = 100,
    progress: (@Sendable (Int, Int) -> Void)? = nil
  ) async throws -> Int {
    guard let chunkAnalyzer else {
      print("[RAG] No chunk analyzer configured — skipping analysis")
      return 0
    }

    try openIfNeeded()

    let sql: String
    let resolvedRepoId: String?
    if let repoPath {
      resolvedRepoId = try resolveRepoId(for: repoPath)
      sql = """
        SELECT c.id, c.text, c.construct_type, c.construct_name, f.language
        FROM chunks c
        JOIN files f ON c.file_id = f.id
        JOIN repos r ON f.repo_id = r.id
        WHERE (c.ai_summary IS NULL OR c.analyzer_model = ? OR c.ai_summary = ?) AND r.id = ?
        LIMIT ?
        """
    } else {
      resolvedRepoId = nil
      sql = """
        SELECT c.id, c.text, c.construct_type, c.construct_name, f.language
        FROM chunks c
        JOIN files f ON c.file_id = f.id
        WHERE c.ai_summary IS NULL OR c.analyzer_model = ? OR c.ai_summary = ?
        LIMIT ?
        """
    }

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }

    var bindIndex: Int32 = 1
    bindText(statement, bindIndex, Self.failedAnalysisModel)
    bindIndex += 1
    bindText(statement, bindIndex, Self.failedAnalysisSummary)
    bindIndex += 1
    if let resolvedRepoId { bindText(statement, bindIndex, resolvedRepoId); bindIndex += 1 }
    sqlite3_bind_int(statement, bindIndex, Int32(limit))

    struct ChunkToAnalyze {
      let id: String
      let text: String
      let constructType: String?
      let constructName: String?
      let language: String?
    }

    var chunksToAnalyze: [ChunkToAnalyze] = []
    while sqlite3_step(statement) == SQLITE_ROW {
      let id = String(cString: sqlite3_column_text(statement, 0))
      let text = String(cString: sqlite3_column_text(statement, 1))
      let constructType = sqlite3_column_text(statement, 2).map { String(cString: $0) }
      let constructName = sqlite3_column_text(statement, 3).map { String(cString: $0) }
      let language = sqlite3_column_text(statement, 4).map { String(cString: $0) }
      chunksToAnalyze.append(ChunkToAnalyze(id: id, text: text, constructType: constructType, constructName: constructName, language: language))
    }

    guard !chunksToAnalyze.isEmpty else { return 0 }

    let now = dateFormatter.string(from: Date())
    var analyzedCount = 0

    for (index, chunk) in chunksToAnalyze.enumerated() {
      progress?(index + 1, chunksToAnalyze.count)

      do {
        let analysis = try await chunkAnalyzer.analyze(
          chunk: chunk.text,
          constructType: chunk.constructType,
          constructName: chunk.constructName,
          language: chunk.language
        )

        let tagsJson = (try? JSONEncoder().encode(analysis.tags)).flatMap { String(data: $0, encoding: .utf8) }
        try updateChunkAnalysis(
          chunkId: chunk.id,
          chunkText: chunk.text,
          aiSummary: analysis.summary,
          aiTags: tagsJson,
          analyzedAt: now,
          analyzerModel: chunkAnalyzer.analyzerName
        )
        analyzedCount += 1
      } catch {
        print("[RAG] Chunk analysis failed for \(chunk.id): \(error)")
        // Persist the failure, but keep it retryable in later analyze passes.
        try? updateChunkAnalysis(
          chunkId: chunk.id,
          chunkText: chunk.text,
          aiSummary: Self.failedAnalysisSummary,
          aiTags: nil,
          analyzedAt: now,
          analyzerModel: Self.failedAnalysisModel
        )
      }
    }

    return analyzedCount
  }

  /// Update chunk with AI analysis results and cache for future reindexes.
  internal func updateChunkAnalysis(
    chunkId: String,
    chunkText: String,
    aiSummary: String,
    aiTags: String?,
    analyzedAt: String,
    analyzerModel: String
  ) throws {
    // Update chunks table (backward compat — still the primary read path)
    let sql = """
      UPDATE chunks SET ai_summary = ?, ai_tags = ?, analyzed_at = ?, analyzer_model = ?, enriched_at = NULL
      WHERE id = ?
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, aiSummary)
      bindTextOrNull(stmt, 2, aiTags)
      bindText(stmt, 3, analyzedAt)
      bindText(stmt, 4, analyzerModel)
      bindText(stmt, 5, chunkId)
    }

    // Also write to chunk_analysis table (multi-model storage #584)
    let insertAnalysis = """
      INSERT OR REPLACE INTO chunk_analysis (chunk_id, analyzer_model, ai_summary, ai_tags, analyzed_at, source)
      VALUES (?, ?, ?, ?, ?, 'local')
      """
    try execute(sql: insertAnalysis) { stmt in
      bindText(stmt, 1, chunkId)
      bindText(stmt, 2, analyzerModel)
      bindText(stmt, 3, aiSummary)
      bindTextOrNull(stmt, 4, aiTags)
      bindText(stmt, 5, analyzedAt)
    }

    guard analyzerModel != Self.failedAnalysisModel, aiSummary != Self.failedAnalysisSummary else {
      return
    }

    let textHash = VectorMath.stableId(for: chunkText)
    try upsertAIAnalysisCache(textHash: textHash, summary: aiSummary, tags: aiTags ?? "[]", model: analyzerModel, cachedAt: analyzedAt)
  }

  /// Get count of un-analyzed chunks.
  public func getUnanalyzedChunkCount(repoPath: String? = nil) throws -> Int {
    try openIfNeeded()
    if let repoPath {
      let resolvedRepoId = try resolveRepoId(for: repoPath)
      return try queryInt("""
        SELECT COUNT(*) FROM chunks c
        JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id
        WHERE (c.ai_summary IS NULL OR c.analyzer_model = ? OR c.ai_summary = ?) AND r.id = ?
        """, bind: { stmt in
          bindText(stmt, 1, Self.failedAnalysisModel)
          bindText(stmt, 2, Self.failedAnalysisSummary)
          bindText(stmt, 3, resolvedRepoId)
        })
    }
    return try queryInt(
      "SELECT COUNT(*) FROM chunks WHERE ai_summary IS NULL OR analyzer_model = ? OR ai_summary = ?",
      bind: { stmt in
        bindText(stmt, 1, Self.failedAnalysisModel)
        bindText(stmt, 2, Self.failedAnalysisSummary)
      }
    )
  }

  /// Get count of analyzed chunks.
  public func getAnalyzedChunkCount(repoPath: String? = nil) throws -> Int {
    try openIfNeeded()
    if let repoPath {
      let resolvedRepoId = try resolveRepoId(for: repoPath)
      return try queryInt("""
        SELECT COUNT(*) FROM chunks c
        JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id
        WHERE c.ai_summary IS NOT NULL
          AND c.analyzer_model != ?
          AND c.ai_summary != ?
          AND r.id = ?
        """, bind: { stmt in
          bindText(stmt, 1, Self.failedAnalysisModel)
          bindText(stmt, 2, Self.failedAnalysisSummary)
          bindText(stmt, 3, resolvedRepoId)
        })
    }
    return try queryInt(
      "SELECT COUNT(*) FROM chunks WHERE ai_summary IS NOT NULL AND analyzer_model != ? AND ai_summary != ?",
      bind: { stmt in
        bindText(stmt, 1, Self.failedAnalysisModel)
        bindText(stmt, 2, Self.failedAnalysisSummary)
      }
    )
  }

  /// Clear AI analysis for all chunks in a repo (or all repos if nil).
  public func clearAnalysis(repoPath: String? = nil) throws {
    try openIfNeeded()
    if let repoPath {
      let resolvedRepoId = try resolveRepoId(for: repoPath)
      try execute(sql: """
        UPDATE chunks SET ai_summary = NULL, ai_tags = NULL, analyzed_at = NULL, analyzer_model = NULL, enriched_at = NULL
        WHERE file_id IN (SELECT f.id FROM files f JOIN repos r ON f.repo_id = r.id WHERE r.id = ?)
        """) { stmt in self.bindText(stmt, 1, resolvedRepoId) }
    } else {
      try exec("UPDATE chunks SET ai_summary = NULL, ai_tags = NULL, analyzed_at = NULL, analyzer_model = NULL, enriched_at = NULL")
    }
  }

  // MARK: - Embedding Enrichment

  /// Re-embed analyzed chunks using enriched text (code + AI summary).
  /// This makes vector search capture both code structure AND semantic meaning.
  public func enrichEmbeddings(
    repoPath: String? = nil,
    limit: Int = 500,
    progress: (@Sendable (Int, Int) -> Void)? = nil
  ) async throws -> Int {
    try openIfNeeded()

    let sql: String
    let resolvedRepoId: String?
    if let repoPath {
      resolvedRepoId = try resolveRepoId(for: repoPath)
      sql = """
        SELECT c.id, c.text, c.ai_summary FROM chunks c
        JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id
        WHERE c.ai_summary IS NOT NULL
          AND c.analyzer_model != ?
          AND c.ai_summary != ?
          AND c.enriched_at IS NULL
          AND r.id = ?
        LIMIT ?
        """
    } else {
      resolvedRepoId = nil
      sql = """
        SELECT c.id, c.text, c.ai_summary FROM chunks c
        WHERE c.ai_summary IS NOT NULL
          AND c.analyzer_model != ?
          AND c.ai_summary != ?
          AND c.enriched_at IS NULL
        LIMIT ?
        """
    }

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }

    var bindIndex: Int32 = 1
    bindText(statement, bindIndex, Self.failedAnalysisModel)
    bindIndex += 1
    bindText(statement, bindIndex, Self.failedAnalysisSummary)
    bindIndex += 1
    if let resolvedRepoId { bindText(statement, bindIndex, resolvedRepoId); bindIndex += 1 }
    sqlite3_bind_int(statement, bindIndex, Int32(limit))

    struct ChunkToEnrich { let id: String; let text: String; let aiSummary: String }
    var chunks: [ChunkToEnrich] = []
    while sqlite3_step(statement) == SQLITE_ROW {
      let id = String(cString: sqlite3_column_text(statement, 0))
      let text = String(cString: sqlite3_column_text(statement, 1))
      let summary = String(cString: sqlite3_column_text(statement, 2))
      chunks.append(ChunkToEnrich(id: id, text: text, aiSummary: summary))
    }

    guard !chunks.isEmpty else { return 0 }

    let enrichedTexts = chunks.map { "\($0.text)\n\n// AI Summary: \($0.aiSummary)" }
    var enrichedCount = 0
    let now = dateFormatter.string(from: Date())

    for range in Self.embedBatchRanges(for: enrichedTexts) {
      let batchTexts = Array(enrichedTexts[range])
      let batchChunks = Array(chunks[range])

      progress?(range.lowerBound, chunks.count)

      // A batch that fails takes nobody down with it: retry its members one at
      // a time so a single unembeddable chunk can't strand the rest. Without
      // this the throw escaped `enrichEmbeddings` entirely and — because the
      // select is a bare LIMIT with no cursor — the very same batch came back
      // next run, forever. tio-api sat at 5485/6484 permanently that way.
      var embeddings = await embed(batchTexts, describedAs: "batch of \(batchTexts.count)")
      if embeddings == nil {
        embeddings = []
        for (offset, text) in batchTexts.enumerated() {
          let single = await embed([text], describedAs: "chunk \(batchChunks[offset].id)")
          // Keep positions aligned with batchChunks — an empty vector is
          // skipped by the writer below, same as a provider returning one.
          embeddings?.append(single?.first ?? [])
        }
      }

      for (offset, vector) in (embeddings ?? []).enumerated() {
        guard !vector.isEmpty, offset < batchChunks.count else { continue }
        let chunk = batchChunks[offset]
        try upsertEmbedding(chunkId: chunk.id, vector: vector)
        try execute(sql: "UPDATE chunks SET enriched_at = ? WHERE id = ?") { stmt in
          self.bindText(stmt, 1, now)
          self.bindText(stmt, 2, chunk.id)
        }
        enrichedCount += 1
      }

      if let batchAware = embeddingProvider as? BatchAwareEmbeddingProvider {
        await batchAware.didCompleteBatch()
      }
      await memoryMonitor.clearCaches()
    }

    return enrichedCount
  }

  /// One embed call, nil on failure. Errors are logged and swallowed so the
  /// caller can degrade (batch → singles) instead of aborting the whole pass.
  private func embed(_ texts: [String], describedAs label: String) async -> [[Float]]? {
    do {
      return try await embeddingProvider.embed(texts: texts)
    } catch {
      print("[RAG] Embedding failed for \(label): \(error)")
      return nil
    }
  }

  /// Largest total payload we'll put in one embed request, in UTF-8 bytes.
  ///
  /// Sized against the failure this exists to prevent: llama.cpp-backed
  /// embedding servers (ollama ≤ 0.31.x among them) kill the model runner
  /// mid-request on an oversized batch, surfacing as a connection EOF rather
  /// than a clean 4xx — so there's nothing to retry against. Measured on
  /// qwen3-embedding:0.6b, a 161 KB request killed the runner every time while
  /// the same chunks sent individually all succeeded; failures started
  /// appearing intermittently around 40 KB. 32 KB keeps a margin under that.
  static let maxEmbedBatchBytes = 32_000

  /// Upper bound on batch *count*, independent of size — a batch of thousands
  /// of tiny chunks is its own kind of trouble.
  static let maxEmbedBatchCount = 32

  /// Split `texts` into contiguous ranges that respect both caps. A single text
  /// over the byte cap gets a range to itself rather than being dropped or
  /// truncated: oversized-but-alone is exactly the shape that empirically
  /// still succeeds, and silently mangling a chunk's embedding is worse than
  /// one risky request.
  static func embedBatchRanges(for texts: [String]) -> [Range<Int>] {
    var ranges: [Range<Int>] = []
    var start = 0
    var bytes = 0
    for index in texts.indices {
      let size = texts[index].utf8.count
      let wouldExceedBytes = bytes > 0 && bytes + size > maxEmbedBatchBytes
      let wouldExceedCount = index - start >= maxEmbedBatchCount
      if wouldExceedBytes || wouldExceedCount {
        ranges.append(start..<index)
        start = index
        bytes = 0
      }
      bytes += size
    }
    if start < texts.count { ranges.append(start..<texts.count) }
    return ranges
  }

  /// Get count of un-enriched chunks (analyzed but not yet re-embedded).
  public func getUnenrichedChunkCount(repoPath: String? = nil) throws -> Int {
    try openIfNeeded()
    if let repoPath {
      let resolvedRepoId = try resolveRepoId(for: repoPath)
      return try queryInt("""
        SELECT COUNT(*) FROM chunks c
        JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id
        WHERE c.ai_summary IS NOT NULL
          AND c.analyzer_model != ?
          AND c.ai_summary != ?
          AND c.enriched_at IS NULL
          AND r.id = ?
        """, bind: { stmt in
          bindText(stmt, 1, Self.failedAnalysisModel)
          bindText(stmt, 2, Self.failedAnalysisSummary)
          bindText(stmt, 3, resolvedRepoId)
        })
    }
    return try queryInt(
      "SELECT COUNT(*) FROM chunks WHERE ai_summary IS NOT NULL AND analyzer_model != ? AND ai_summary != ? AND enriched_at IS NULL",
      bind: { stmt in
        bindText(stmt, 1, Self.failedAnalysisModel)
        bindText(stmt, 2, Self.failedAnalysisSummary)
      }
    )
  }

  /// Get count of enriched chunks.
  public func getEnrichedChunkCount(repoPath: String? = nil) throws -> Int {
    try openIfNeeded()
    if let repoPath {
      let resolvedRepoId = try resolveRepoId(for: repoPath)
      return try queryInt("""
        SELECT COUNT(*) FROM chunks c
        JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id
        WHERE c.enriched_at IS NOT NULL
          AND c.analyzer_model != ?
          AND c.ai_summary != ?
          AND r.id = ?
        """, bind: { stmt in
          bindText(stmt, 1, Self.failedAnalysisModel)
          bindText(stmt, 2, Self.failedAnalysisSummary)
          bindText(stmt, 3, resolvedRepoId)
        })
    }
    return try queryInt(
      "SELECT COUNT(*) FROM chunks WHERE enriched_at IS NOT NULL AND analyzer_model != ? AND ai_summary != ?",
      bind: { stmt in
        bindText(stmt, 1, Self.failedAnalysisModel)
        bindText(stmt, 2, Self.failedAnalysisSummary)
      }
    )
  }

  // MARK: - Code Analytics

  /// Find duplicate code constructs across files.
  public func findDuplicates(
    repoPath: String? = nil,
    minTokens: Int = 50,
    limit: Int = 20
  ) throws -> [DuplicateGroup] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let resolvedRepoId: String? = if let repoPath { try resolveRepoId(for: repoPath) } else { nil }

    // Two-level aggregation so each group carries its file paths + per-file
    // token sums (the flat GROUP BY couldn't — `files` always came back
    // empty, cloke/peel#774): inner query collapses chunks to one row per
    // (construct, file), outer groups files into constructs and packs the
    // file list with GROUP_CONCAT (char(31) unit separator between path and
    // tokens, char(10) between files — neither appears in paths).
    var sql = """
      SELECT construct_name, construct_type,
             COUNT(*) as file_count,
             SUM(file_tokens) as total_tokens,
             MAX(any_summary) as ai_summary,
             GROUP_CONCAT(path || char(31) || file_tokens, char(10)) as file_list
      FROM (
        SELECT c.construct_name, c.construct_type, f.path,
               SUM(c.token_count) as file_tokens,
               MAX(c.ai_summary) as any_summary
        FROM chunks c
        JOIN files f ON c.file_id = f.id
        JOIN repos r ON f.repo_id = r.id
        WHERE c.construct_name IS NOT NULL AND c.token_count >= ?
      """
    if resolvedRepoId != nil { sql += " AND r.id = ?" }
    sql += """
        GROUP BY c.construct_name, c.construct_type, f.path
      )
      GROUP BY construct_name, construct_type
      HAVING COUNT(*) > 1
      ORDER BY total_tokens DESC
      LIMIT ?
      """

    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }

    var bindIdx: Int32 = 1
    sqlite3_bind_int(stmt, bindIdx, Int32(minTokens)); bindIdx += 1
    if let resolvedRepoId { bindText(stmt, bindIdx, resolvedRepoId); bindIdx += 1 }
    sqlite3_bind_int(stmt, bindIdx, Int32(limit))

    var groups: [DuplicateGroup] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let name = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let type = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
      let fileCount = Int(sqlite3_column_int(stmt, 2))
      let totalTokens = Int(sqlite3_column_int(stmt, 3))
      let aiSummary = sqlite3_column_text(stmt, 4).map { String(cString: $0) }
      let fileList = sqlite3_column_text(stmt, 5).map { String(cString: $0) } ?? ""
      let files: [(path: String, tokenCount: Int)] = fileList
        .split(separator: "\n")
        .compactMap { entry in
          let parts = entry.split(separator: "\u{1F}", maxSplits: 1)
          guard parts.count == 2, let tokens = Int(parts[1]) else { return nil }
          return (path: String(parts[0]), tokenCount: tokens)
        }
        .sorted { $0.tokenCount > $1.tokenCount }

      groups.append(DuplicateGroup(
        constructName: name,
        constructType: type,
        fileCount: fileCount,
        totalTokens: totalTokens,
        wastedTokens: totalTokens - (totalTokens / fileCount),
        aiSummary: aiSummary,
        files: files
      ))
    }

    return groups
  }

  /// Find naming pattern groups (e.g., all `*ViewModel`, `*Service`, etc.).
  public func findPatterns(
    repoPath: String? = nil,
    limit: Int = 20
  ) throws -> [PatternGroup] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let resolvedRepoId: String? = if let repoPath { try resolveRepoId(for: repoPath) } else { nil }

    var sql = """
      SELECT c.construct_name, c.construct_type, f.path, c.token_count
      FROM chunks c
      JOIN files f ON c.file_id = f.id
      JOIN repos r ON f.repo_id = r.id
      WHERE c.construct_name IS NOT NULL AND c.construct_type IS NOT NULL
      """
    if resolvedRepoId != nil { sql += " AND r.id = ?" }
    sql += " ORDER BY c.construct_name"

    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }

    if let resolvedRepoId { bindText(stmt, 1, resolvedRepoId) }

    // Collect construct names and group by suffixes
    let commonSuffixes = ["ViewModel", "View", "Service", "Controller", "Manager", "Handler", "Provider", "Factory", "Helper", "Delegate", "DataSource", "Store", "Router", "Coordinator", "Presenter", "Interactor", "UseCase", "Repository", "Adapter", "Builder", "Configurator", "Validator", "Formatter", "Parser", "Serializer"]

    var suffixGroups: [String: [(constructName: String, path: String, tokenCount: Int)]] = [:]

    while sqlite3_step(stmt) == SQLITE_ROW {
      let name = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let path = sqlite3_column_text(stmt, 2).map { String(cString: $0) } ?? ""
      let tokenCount = Int(sqlite3_column_int(stmt, 3))

      for suffix in commonSuffixes {
        if name.hasSuffix(suffix) && name.count > suffix.count {
          suffixGroups[suffix, default: []].append((name, path, tokenCount))
          break
        }
      }
    }

    return suffixGroups
      .filter { $0.value.count >= 2 }
      .map { (suffix, items) in
        PatternGroup(
          suffix: suffix,
          count: items.count,
          totalTokens: items.reduce(0) { $0 + $1.tokenCount },
          samples: Array(items.prefix(5))
        )
      }
      .sorted { $0.count > $1.count }
      .prefix(limit)
      .map { $0 }
  }

  /// Find hotspots: constructs exceeding a token threshold.
  public func findHotspots(
    repoPath: String? = nil,
    tokenThreshold: Int = 200,
    limit: Int = 20
  ) throws -> [Hotspot] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let resolvedRepoId: String? = if let repoPath { try resolveRepoId(for: repoPath) } else { nil }

    var sql = """
      SELECT c.construct_name, c.construct_type, f.path, c.token_count,
             c.start_line, c.end_line, c.ai_summary, c.ai_tags
      FROM chunks c
      JOIN files f ON c.file_id = f.id
      JOIN repos r ON f.repo_id = r.id
      WHERE c.token_count >= ?
      """
    if resolvedRepoId != nil { sql += " AND r.id = ?" }
    sql += " ORDER BY c.token_count DESC LIMIT ?"

    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }

    var bindIdx: Int32 = 1
    sqlite3_bind_int(stmt, bindIdx, Int32(tokenThreshold)); bindIdx += 1
    if let resolvedRepoId { bindText(stmt, bindIdx, resolvedRepoId); bindIdx += 1 }
    sqlite3_bind_int(stmt, bindIdx, Int32(limit))

    var hotspots: [Hotspot] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let name = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? "unknown"
      let type = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? "unknown"
      let path = sqlite3_column_text(stmt, 2).map { String(cString: $0) } ?? ""
      let tokenCount = Int(sqlite3_column_int(stmt, 3))
      let startLine = Int(sqlite3_column_int(stmt, 4))
      let endLine = Int(sqlite3_column_int(stmt, 5))
      let aiSummary = sqlite3_column_text(stmt, 6).map { String(cString: $0) }
      let aiTagsJson = sqlite3_column_text(stmt, 7).map { String(cString: $0) }
      let aiTags: [String] = aiTagsJson.flatMap { json in
        json.data(using: .utf8).flatMap { try? JSONDecoder().decode([String].self, from: $0) }
      } ?? []

      hotspots.append(Hotspot(
        constructName: name, constructType: type, filePath: path,
        tokenCount: tokenCount, startLine: startLine, endLine: endLine,
        aiSummary: aiSummary, aiTags: aiTags
      ))
    }

    return hotspots
  }

  // MARK: - Stats & Facets

  /// Get construct type distribution.
  public func getConstructTypeStats(repoPath: String? = nil) throws -> [(type: String, count: Int)] {
    try openIfNeeded()
    let resolvedRepoId: String? = if let repoPath { try resolveRepoId(for: repoPath) } else { nil }
    let sql: String
    if resolvedRepoId != nil {
      sql = """
        SELECT COALESCE(c.construct_type, 'unknown') as type, COUNT(*) as cnt
        FROM chunks c JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id
        WHERE r.id = ? GROUP BY type ORDER BY cnt DESC
        """
    } else {
      sql = "SELECT COALESCE(construct_type, 'unknown') as type, COUNT(*) as cnt FROM chunks GROUP BY type ORDER BY cnt DESC"
    }
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { throw RAGError.sqlite(String(cString: sqlite3_errmsg(db))) }
    defer { sqlite3_finalize(stmt) }
    if let resolvedRepoId { bindText(stmt, 1, resolvedRepoId) }

    var stats: [(type: String, count: Int)] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      stats.append((String(cString: sqlite3_column_text(stmt, 0)), Int(sqlite3_column_int(stmt, 1))))
    }
    return stats
  }

  /// Get facet counts for filtering/grouping.
  public func getFacets(repoPath: String? = nil) throws -> FacetCounts {
    try openIfNeeded()
    let resolvedRepoId: String? = if let repoPath { try resolveRepoId(for: repoPath) } else { nil }

    func facetQuery(_ sql: String) throws -> [(String, Int)] {
      guard let db else { throw RAGError.sqlite("Database not initialized") }
      var stmt: OpaquePointer?
      let r = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
      guard r == SQLITE_OK, let stmt else { throw RAGError.sqlite(String(cString: sqlite3_errmsg(db))) }
      defer { sqlite3_finalize(stmt) }
      if let resolvedRepoId { bindText(stmt, 1, resolvedRepoId) }
      var results: [(String, Int)] = []
      while sqlite3_step(stmt) == SQLITE_ROW {
        results.append((String(cString: sqlite3_column_text(stmt, 0)), Int(sqlite3_column_int(stmt, 1))))
      }
      return results
    }

    let moduleSql = resolvedRepoId != nil
      ? "SELECT f.module_path, COUNT(*) FROM files f JOIN repos r ON f.repo_id = r.id WHERE r.id = ? AND f.module_path IS NOT NULL GROUP BY f.module_path ORDER BY COUNT(*) DESC"
      : "SELECT module_path, COUNT(*) FROM files WHERE module_path IS NOT NULL GROUP BY module_path ORDER BY COUNT(*) DESC"
    let langSql = resolvedRepoId != nil
      ? "SELECT f.language, COUNT(*) FROM files f JOIN repos r ON f.repo_id = r.id WHERE r.id = ? AND f.language IS NOT NULL GROUP BY f.language ORDER BY COUNT(*) DESC"
      : "SELECT language, COUNT(*) FROM files WHERE language IS NOT NULL GROUP BY language ORDER BY COUNT(*) DESC"
    let ctSql = resolvedRepoId != nil
      ? "SELECT COALESCE(c.construct_type, 'unknown'), COUNT(*) FROM chunks c JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id WHERE r.id = ? GROUP BY c.construct_type ORDER BY COUNT(*) DESC"
      : "SELECT COALESCE(construct_type, 'unknown'), COUNT(*) FROM chunks GROUP BY construct_type ORDER BY COUNT(*) DESC"

    let modules = try facetQuery(moduleSql)
    let languages = try facetQuery(langSql)
    let constructTypes = try facetQuery(ctSql)

    // Feature tags need JSON parsing
    let featureTags = try queryFeatureTagCounts(resolvedRepoId: resolvedRepoId)

    return FacetCounts(
      modulePaths: modules.map { ($0.0, $0.1) },
      featureTags: featureTags,
      languages: languages.map { ($0.0, $0.1) },
      constructTypes: constructTypes.map { ($0.0, $0.1) }
    )
  }

  internal func queryFeatureTagCounts(resolvedRepoId: String?) throws -> [(tag: String, count: Int)] {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = resolvedRepoId != nil
      ? "SELECT f.feature_tags FROM files f JOIN repos r ON f.repo_id = r.id WHERE r.id = ? AND f.feature_tags IS NOT NULL"
      : "SELECT feature_tags FROM files WHERE feature_tags IS NOT NULL"

    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { throw RAGError.sqlite(String(cString: sqlite3_errmsg(db))) }
    defer { sqlite3_finalize(stmt) }
    if let resolvedRepoId { bindText(stmt, 1, resolvedRepoId) }

    var tagCounts: [String: Int] = [:]
    while sqlite3_step(stmt) == SQLITE_ROW {
      let json = String(cString: sqlite3_column_text(stmt, 0))
      if let data = json.data(using: .utf8),
         let tags = try? JSONDecoder().decode([String].self, from: data) {
        for tag in tags { tagCounts[tag, default: 0] += 1 }
      }
    }
    return tagCounts.map { ($0.key, $0.value) }.sorted { $0.1 > $1.1 }
  }

  /// Get largest files by chunk count.
  public func getLargeFiles(repoPath: String? = nil, limit: Int = 20) throws -> [(path: String, chunkCount: Int, totalLines: Int, language: String?)] {
    try openIfNeeded()
    let resolvedRepoId: String? = if let repoPath { try resolveRepoId(for: repoPath) } else { nil }
    let sql: String
    if resolvedRepoId != nil {
      sql = """
        SELECT r.root_path || '/' || f.path, COUNT(*), SUM(c.end_line - c.start_line), f.language
        FROM chunks c JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id
        WHERE r.id = ? GROUP BY f.id ORDER BY SUM(c.end_line - c.start_line) DESC LIMIT ?
        """
    } else {
      sql = """
        SELECT r.root_path || '/' || f.path, COUNT(*), SUM(c.end_line - c.start_line), f.language
        FROM chunks c JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id
        GROUP BY f.id ORDER BY SUM(c.end_line - c.start_line) DESC LIMIT ?
        """
    }

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { throw RAGError.sqlite(String(cString: sqlite3_errmsg(db))) }
    defer { sqlite3_finalize(stmt) }

    if let resolvedRepoId { bindText(stmt, 1, resolvedRepoId); sqlite3_bind_int(stmt, 2, Int32(limit)) }
    else { sqlite3_bind_int(stmt, 1, Int32(limit)) }

    var files: [(path: String, chunkCount: Int, totalLines: Int, language: String?)] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let path = String(cString: sqlite3_column_text(stmt, 0))
      let cc = Int(sqlite3_column_int(stmt, 1))
      let tl = Int(sqlite3_column_int(stmt, 2))
      let lang: String? = sqlite3_column_type(stmt, 3) != SQLITE_NULL ? String(cString: sqlite3_column_text(stmt, 3)) : nil
      files.append((path, cc, tl, lang))
    }
    return files
  }

  /// Get index stats.
  public func getIndexStats(repoPath: String? = nil) throws -> (fileCount: Int, chunkCount: Int, embeddingCount: Int, totalLines: Int) {
    try openIfNeeded()
    if let repoPath {
      let resolvedRepoId = try resolveRepoId(for: repoPath)
      let fc = try queryInt("SELECT COUNT(*) FROM files f JOIN repos r ON f.repo_id = r.id WHERE r.id = ?", bind: { s in bindText(s, 1, resolvedRepoId) })
      let cc = try queryInt("SELECT COUNT(*) FROM chunks c JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id WHERE r.id = ?", bind: { s in bindText(s, 1, resolvedRepoId) })
      let ec = try queryInt("SELECT COUNT(*) FROM embeddings e JOIN chunks c ON e.chunk_id = c.id JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id WHERE r.id = ?", bind: { s in bindText(s, 1, resolvedRepoId) })
      let tl = try queryInt("SELECT COALESCE(SUM(c.end_line - c.start_line), 0) FROM chunks c JOIN files f ON c.file_id = f.id JOIN repos r ON f.repo_id = r.id WHERE r.id = ?", bind: { s in bindText(s, 1, resolvedRepoId) })
      return (fc, cc, ec, tl)
    }
    return (
      try queryInt("SELECT COUNT(*) FROM files"),
      try queryInt("SELECT COUNT(*) FROM chunks"),
      try queryInt("SELECT COUNT(*) FROM embeddings"),
      try queryInt("SELECT COALESCE(SUM(end_line - start_line), 0) FROM chunks")
    )
  }

  // MARK: - Multi-Model Analysis (#584)

  /// List all analyzer models that have analysis for a given repo.
  public func availableAnalyzerModels(repoPath: String) throws -> [String] {
    try openIfNeeded()
    try ensureSchema()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    let repoId = try resolveRepoId(for: repoPath)
    let sql = """
      SELECT DISTINCT ca.analyzer_model
      FROM chunk_analysis ca
      JOIN chunks c ON ca.chunk_id = c.id
      JOIN files f ON c.file_id = f.id
      WHERE f.repo_id = ?
      ORDER BY ca.analyzer_model
      """
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return [] }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, repoId)

    var models: [String] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      if let text = sqlite3_column_text(stmt, 0) {
        models.append(String(cString: text))
      }
    }
    return models
  }

  /// Count of analysis records per model for a repo.
  public func analysisCountByModel(repoPath: String) throws -> [(model: String, count: Int)] {
    try openIfNeeded()
    try ensureSchema()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    let repoId = try resolveRepoId(for: repoPath)
    let sql = """
      SELECT ca.analyzer_model, COUNT(*)
      FROM chunk_analysis ca
      JOIN chunks c ON ca.chunk_id = c.id
      JOIN files f ON c.file_id = f.id
      WHERE f.repo_id = ?
      GROUP BY ca.analyzer_model
      ORDER BY COUNT(*) DESC
      """
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return [] }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, repoId)

    var results: [(model: String, count: Int)] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      if let model = sqlite3_column_text(stmt, 0) {
        results.append((String(cString: model), Int(sqlite3_column_int(stmt, 1))))
      }
    }
    return results
  }

  /// Import analysis from an overlay (remote peer) without overwriting local analysis.
  /// Each model's analysis is stored in its own row — no conflicts.
  public func importAnalysisFromOverlay(
    chunkId: String,
    analyzerModel: String,
    aiSummary: String,
    aiTags: String?,
    analyzedAt: String,
    source: String
  ) throws {
    try openIfNeeded()
    try ensureSchema()

    let sql = """
      INSERT OR REPLACE INTO chunk_analysis (chunk_id, analyzer_model, ai_summary, ai_tags, analyzed_at, source)
      VALUES (?, ?, ?, ?, ?, ?)
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, chunkId)
      bindText(stmt, 2, analyzerModel)
      bindText(stmt, 3, aiSummary)
      bindTextOrNull(stmt, 4, aiTags)
      bindText(stmt, 5, analyzedAt)
      bindText(stmt, 6, source)
    }
  }

  // MARK: - Analyzer Pin

  /// How one repository's analysis is distributed across analyzer models.
  ///
  /// The distinction that matters: `pinnedModel` is what this corpus is
  /// *supposed* to be analyzed with; `coverage` is what it actually is. A
  /// corpus whose chunks were each analyzed once, by whatever model happened
  /// to be configured that day, reads as one repository but behaves like
  /// several — summaries of very different quality ranked against each other
  /// as equals, and no amount of re-running analysis converges it.
  public struct AnalyzerDriftSummary: Sendable, Equatable {
    /// The repo's analyzer pin; nil when unpinned.
    public let pinnedModel: String?
    /// Chunk count per analyzer model, descending. Excludes failures.
    public let coverage: [ModelCount]
    /// Chunks analyzed by a model other than the pin. Zero when unpinned.
    public let offPinChunks: Int
    /// Chunks whose analysis failed. Counted separately because they are
    /// recorded under a pseudo-model and otherwise inflate "analyzed".
    public let failedChunks: Int
    /// Chunks with no analysis at all.
    public let unanalyzedChunks: Int

    public struct ModelCount: Sendable, Equatable {
      public let model: String
      public let count: Int
      public init(model: String, count: Int) {
        self.model = model
        self.count = count
      }
    }

    /// True when the corpus is a blend rather than one analyzer's work.
    public var isDrifted: Bool { coverage.count > 1 }
  }

  /// Read a repository's analyzer pin. nil when unpinned.
  public func repoAnalyzerModel(repoPath: String) throws -> String? {
    try openIfNeeded()
    try ensureSchema()
    let repoId = try resolveRepoId(for: repoPath)
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    var statement: OpaquePointer?
    guard sqlite3_prepare_v2(db, "SELECT analyzer_model FROM repos WHERE id = ?", -1, &statement, nil) == SQLITE_OK,
          let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }
    bindText(statement, 1, repoId)
    guard sqlite3_step(statement) == SQLITE_ROW,
          let raw = sqlite3_column_text(statement, 0) else { return nil }
    let value = String(cString: raw).trimmingCharacters(in: .whitespacesAndNewlines)
    return value.isEmpty ? nil : value
  }

  /// Pin a repository to an analyzer model, or pass nil to unpin.
  ///
  /// Setting the pin declares intent; it re-analyzes nothing. Read
  /// `analyzerDrift` to see the gap it exposes.
  public func setRepoAnalyzerModel(repoPath: String, model: String?) throws {
    try openIfNeeded()
    try ensureSchema()
    let repoId = try resolveRepoId(for: repoPath)
    let normalized = model?.trimmingCharacters(in: .whitespacesAndNewlines)
    let resolved = (normalized?.isEmpty ?? true) ? nil : normalized
    try execute(sql: "UPDATE repos SET analyzer_model = ? WHERE id = ?") { stmt in
      self.bindTextOrNull(stmt, 1, resolved)
      self.bindText(stmt, 2, repoId)
    }
  }

  /// What this corpus was actually analyzed with, versus what it is pinned to.
  public func analyzerDrift(repoPath: String) throws -> AnalyzerDriftSummary {
    try openIfNeeded()
    try ensureSchema()
    let repoId = try resolveRepoId(for: repoPath)
    let pin = try repoAnalyzerModel(repoPath: repoPath)
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    var coverage: [AnalyzerDriftSummary.ModelCount] = []
    var failed = 0
    let sql = """
      SELECT COALESCE(c.analyzer_model, 'unknown'), COUNT(*)
      FROM chunks c JOIN files f ON c.file_id = f.id
      WHERE f.repo_id = ? AND c.ai_summary IS NOT NULL
      GROUP BY 1 ORDER BY 2 DESC
      """
    var statement: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }
    bindText(statement, 1, repoId)
    while sqlite3_step(statement) == SQLITE_ROW {
      let model = String(cString: sqlite3_column_text(statement, 0))
      let count = Int(sqlite3_column_int(statement, 1))
      // Failures are recorded under a pseudo-model. Reporting them as if they
      // were an analyzer is what makes "analyzed" read higher than the usable
      // analysis actually present.
      if model == Self.failedAnalysisModel {
        failed += count
      } else {
        coverage.append(.init(model: model, count: count))
      }
    }

    let offPin = pin.map { pinned in
      coverage.filter { $0.model != pinned }.reduce(0) { $0 + $1.count }
    } ?? 0

    let unanalyzed = try queryInt("""
      SELECT COUNT(*) FROM chunks c JOIN files f ON c.file_id = f.id
      WHERE f.repo_id = ? AND c.ai_summary IS NULL
      """) { stmt in self.bindText(stmt, 1, repoId) }

    return AnalyzerDriftSummary(
      pinnedModel: pin,
      coverage: coverage,
      offPinChunks: offPin,
      failedChunks: failed,
      unanalyzedChunks: unanalyzed
    )
  }
}
