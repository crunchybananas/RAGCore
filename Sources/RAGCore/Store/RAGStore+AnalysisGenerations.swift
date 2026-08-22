//
//  RAGStore+AnalysisGenerations.swift
//  RAGCore
//
//  Blue/green AI analysis. A replacement generation is analyzed and enriched
//  in durable staging tables while every read continues to serve the active
//  chunk and embedding rows. Promotion is one SQLite transaction.
//

import CSQLite
import Foundation

extension RAGStore {
  public enum AnalysisGenerationPhase: String, Sendable, Equatable {
    case analyzing
    case ready
    case committed
  }

  public struct AnalysisGenerationStatus: Sendable, Equatable {
    public let generationId: String
    public let repoId: String
    public let analyzerModel: String
    public let phase: AnalysisGenerationPhase
    public let createdAt: Date
    public let committedAt: Date?
    public let totalChunks: Int
    public let analyzedChunks: Int
    public let enrichedChunks: Int

    public var unanalyzedChunks: Int { max(0, totalChunks - analyzedChunks) }
    public var unenrichedChunks: Int { max(0, analyzedChunks - enrichedChunks) }
    public var isReady: Bool {
      totalChunks > 0 && analyzedChunks == totalChunks && enrichedChunks == totalChunks
    }
  }

  private struct AnalysisGenerationRecord {
    let id: String
    let repoId: String
    let analyzerModel: String
    let phase: AnalysisGenerationPhase
    let createdAt: Date
    let committedAt: Date?
  }

  /// Start a replacement analysis generation, or resume the existing
  /// uncommitted generation for this repository and model after cancellation or
  /// process restart. A different model cannot silently replace staged work;
  /// callers must abort it explicitly first.
  public func beginAnalysisGeneration(
    repoPath: String,
    analyzerModel: String
  ) throws -> AnalysisGenerationStatus {
    try openIfNeeded()
    try ensureSchema()
    let repoId = try resolveRepoId(for: repoPath)
    let model = analyzerModel.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !model.isEmpty else {
      throw RAGError.sqlite("Analysis generation requires a non-empty analyzer model")
    }

    if let existing = try uncommittedAnalysisGeneration(repoId: repoId) {
      guard existing.analyzerModel == model else {
        throw RAGError.sqlite(
          "Repository already has staged analysis generation \(existing.id) for \(existing.analyzerModel); abort it before starting \(model)"
        )
      }
      return try analysisGenerationStatus(record: existing)
    }

    let generationId = UUID().uuidString
    let createdAt = dateFormatter.string(from: Date())
    try execute(sql: """
      INSERT INTO analysis_generations (id, repo_id, analyzer_model, state, created_at)
      VALUES (?, ?, ?, 'analyzing', ?)
      """) { statement in
      bindText(statement, 1, generationId)
      bindText(statement, 2, repoId)
      bindText(statement, 3, model)
      bindText(statement, 4, createdAt)
    }
    return try analysisGenerationStatus(generationId: generationId)
  }

  /// Return durable progress for a staged or committed generation.
  public func analysisGenerationStatus(
    generationId: String
  ) throws -> AnalysisGenerationStatus {
    try openIfNeeded()
    try ensureSchema()
    return try analysisGenerationStatus(record: loadAnalysisGeneration(generationId: generationId))
  }

  /// Return the currently staged generation for a repository, if any.
  public func stagedAnalysisGenerationStatus(
    repoPath: String
  ) throws -> AnalysisGenerationStatus? {
    try openIfNeeded()
    try ensureSchema()
    let repoId = try resolveRepoId(for: repoPath)
    guard let record = try uncommittedAnalysisGeneration(repoId: repoId) else { return nil }
    return try analysisGenerationStatus(record: record)
  }

  /// Analyze chunks into a durable staging generation. Live summaries, tags,
  /// analyzer pin, and embeddings are untouched.
  public func analyzeGeneration(
    generationId: String,
    limit: Int = 100,
    progress: (@Sendable (Int, Int) -> Void)? = nil
  ) async throws -> Int {
    try openIfNeeded()
    try ensureSchema()
    let generation = try loadAnalysisGeneration(generationId: generationId)
    guard generation.phase != .committed else {
      throw RAGError.sqlite("Analysis generation \(generationId) is already committed")
    }
    guard let chunkAnalyzer else {
      throw RAGError.sqlite("No chunk analyzer is configured")
    }
    guard chunkAnalyzer.analyzerName == generation.analyzerModel else {
      throw RAGError.sqlite(
        "Configured analyzer \(chunkAnalyzer.analyzerName) does not match staged generation model \(generation.analyzerModel)"
      )
    }

    struct PendingChunk {
      let id: String
      let text: String
      let constructType: String?
      let constructName: String?
      let language: String?
    }

    let sql = """
      SELECT c.id, c.text, c.construct_type, c.construct_name, f.language
      FROM chunks c
      JOIN files f ON f.id = c.file_id
      WHERE f.repo_id = ?
        AND NOT EXISTS (
          SELECT 1 FROM staged_chunk_analysis s
          WHERE s.generation_id = ? AND s.chunk_id = c.id
        )
      ORDER BY c.id
      LIMIT ?
      """
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var statement: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK,
          let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }
    bindText(statement, 1, generation.repoId)
    bindText(statement, 2, generationId)
    sqlite3_bind_int(statement, 3, Int32(max(1, limit)))

    var pending: [PendingChunk] = []
    while sqlite3_step(statement) == SQLITE_ROW {
      pending.append(PendingChunk(
        id: String(cString: sqlite3_column_text(statement, 0)),
        text: String(cString: sqlite3_column_text(statement, 1)),
        constructType: sqlite3_column_text(statement, 2).map { String(cString: $0) },
        constructName: sqlite3_column_text(statement, 3).map { String(cString: $0) },
        language: sqlite3_column_text(statement, 4).map { String(cString: $0) }
      ))
    }
    guard !pending.isEmpty else { return 0 }

    var analyzed = 0
    for (index, chunk) in pending.enumerated() {
      try Task.checkCancellation()
      progress?(index + 1, pending.count)
      do {
        let analysis = try await chunkAnalyzer.analyze(
          chunk: chunk.text,
          constructType: chunk.constructType,
          constructName: chunk.constructName,
          language: chunk.language
        )
        let tags = try? JSONEncoder().encode(analysis.tags)
        let tagsJSON = tags.flatMap { String(data: $0, encoding: .utf8) }
        let analyzedAt = dateFormatter.string(from: Date())
        try execute(sql: """
          INSERT OR REPLACE INTO staged_chunk_analysis (
            generation_id, chunk_id, text_hash, ai_summary, ai_tags, analyzed_at
          ) VALUES (?, ?, ?, ?, ?, ?)
          """) { staged in
          bindText(staged, 1, generationId)
          bindText(staged, 2, chunk.id)
          bindText(staged, 3, VectorMath.stableId(for: chunk.text))
          bindText(staged, 4, analysis.summary)
          bindTextOrNull(staged, 5, tagsJSON)
          bindText(staged, 6, analyzedAt)
        }
        analyzed += 1
      } catch is CancellationError {
        throw CancellationError()
      } catch {
        // A failed row stays missing, so a later pass can retry it and a
        // generation can never be marked complete with a failure sentinel.
        print("[RAG] Staged chunk analysis failed for \(chunk.id): \(error)")
      }
    }
    return analyzed
  }

  /// Build replacement enriched embeddings from staged summaries. Live vector
  /// search continues to use the current `embeddings`/`vec_chunks` rows.
  public func enrichAnalysisGeneration(
    generationId: String,
    limit: Int = 500,
    progress: (@Sendable (Int, Int) -> Void)? = nil
  ) async throws -> Int {
    try openIfNeeded()
    try ensureSchema()
    let generation = try loadAnalysisGeneration(generationId: generationId)
    guard generation.phase != .committed else {
      throw RAGError.sqlite("Analysis generation \(generationId) is already committed")
    }

    struct PendingEmbedding {
      let chunkId: String
      let text: String
      let summary: String
    }
    let sql = """
      SELECT s.chunk_id, c.text, s.ai_summary
      FROM staged_chunk_analysis s
      JOIN chunks c ON c.id = s.chunk_id
      JOIN files f ON f.id = c.file_id
      WHERE s.generation_id = ? AND f.repo_id = ? AND s.embedding IS NULL
      ORDER BY s.chunk_id
      LIMIT ?
      """
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var statement: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK,
          let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }
    bindText(statement, 1, generationId)
    bindText(statement, 2, generation.repoId)
    sqlite3_bind_int(statement, 3, Int32(max(1, limit)))

    var pending: [PendingEmbedding] = []
    while sqlite3_step(statement) == SQLITE_ROW {
      pending.append(PendingEmbedding(
        chunkId: String(cString: sqlite3_column_text(statement, 0)),
        text: String(cString: sqlite3_column_text(statement, 1)),
        summary: String(cString: sqlite3_column_text(statement, 2))
      ))
    }

    let enrichedTexts = pending.map { "\($0.text)\n\n// AI Summary: \($0.summary)" }
    var enriched = 0
    for range in Self.embedBatchRanges(for: enrichedTexts) {
      try Task.checkCancellation()
      let texts = Array(enrichedTexts[range])
      let chunks = Array(pending[range])
      progress?(range.lowerBound, pending.count)

      var vectors = await embed(texts, describedAs: "staged batch of \(texts.count)")
      if vectors == nil {
        vectors = []
        for (offset, text) in texts.enumerated() {
          let single = await embed([text], describedAs: "staged chunk \(chunks[offset].chunkId)")
          vectors?.append(single?.first ?? [])
        }
      }

      for (offset, vector) in (vectors ?? []).enumerated() {
        guard !vector.isEmpty, offset < chunks.count else { continue }
        let configuredDimensions = embeddingProvider.dimensions
        guard configuredDimensions == 0 || configuredDimensions == vector.count else {
          throw RAGError.embeddingFailed(
            "Embedding has \(vector.count) dimensions; provider declares \(configuredDimensions)"
          )
        }
        let data = VectorMath.encodeVector(vector)
        let enrichedAt = dateFormatter.string(from: Date())
        try execute(sql: """
          UPDATE staged_chunk_analysis SET embedding = ?, enriched_at = ?
          WHERE generation_id = ? AND chunk_id = ?
          """) { staged in
          _ = data.withUnsafeBytes { bytes in
            sqlite3_bind_blob(staged, 1, bytes.baseAddress, Int32(data.count), sqliteTransient)
          }
          bindText(staged, 2, enrichedAt)
          bindText(staged, 3, generationId)
          bindText(staged, 4, chunks[offset].chunkId)
        }
        enriched += 1
      }

      if let batchAware = embeddingProvider as? BatchAwareEmbeddingProvider {
        await batchAware.didCompleteBatch()
      }
      await memoryMonitor.clearCaches()
    }

    let status = try analysisGenerationStatus(generationId: generationId)
    if status.isReady {
      try execute(sql: "UPDATE analysis_generations SET state = 'ready' WHERE id = ?") {
        bindText($0, 1, generationId)
      }
    }
    return enriched
  }

  /// Atomically promote a complete staged generation. Every active summary,
  /// tag, analyzer pin, enriched timestamp, scalar embedding, and sqlite-vec
  /// row changes in the same transaction; an incomplete generation is refused.
  @discardableResult
  public func commitAnalysisGeneration(
    generationId: String
  ) throws -> AnalysisGenerationStatus {
    try openIfNeeded()
    try ensureSchema()
    let generation = try loadAnalysisGeneration(generationId: generationId)
    guard generation.phase != .committed else {
      return try analysisGenerationStatus(record: generation)
    }
    let stagedStatus = try analysisGenerationStatus(record: generation)
    guard stagedStatus.isReady else {
      throw RAGError.sqlite(
        "Analysis generation \(generationId) is incomplete: \(stagedStatus.unanalyzedChunks) unanalyzed, \(stagedStatus.unenrichedChunks) unenriched"
      )
    }

    let minBytes = try queryInt(
      "SELECT COALESCE(MIN(LENGTH(embedding)), 0) FROM staged_chunk_analysis WHERE generation_id = ?",
      bind: { bindText($0, 1, generationId) }
    )
    let maxBytes = try queryInt(
      "SELECT COALESCE(MAX(LENGTH(embedding)), 0) FROM staged_chunk_analysis WHERE generation_id = ?",
      bind: { bindText($0, 1, generationId) }
    )
    guard minBytes > 0, minBytes == maxBytes,
          minBytes.isMultiple(of: MemoryLayout<Float>.size) else {
      throw RAGError.embeddingFailed("Staged generation contains missing or inconsistent embedding dimensions")
    }
    let dimensions = minBytes / MemoryLayout<Float>.size
    if extensionLoaded { try ensureVecTable(dimensions: dimensions) }

    do {
      try exec("BEGIN IMMEDIATE TRANSACTION")

      try execute(sql: """
        UPDATE chunks SET
          ai_summary = (SELECT s.ai_summary FROM staged_chunk_analysis s WHERE s.generation_id = ? AND s.chunk_id = chunks.id),
          ai_tags = (SELECT s.ai_tags FROM staged_chunk_analysis s WHERE s.generation_id = ? AND s.chunk_id = chunks.id),
          analyzed_at = (SELECT s.analyzed_at FROM staged_chunk_analysis s WHERE s.generation_id = ? AND s.chunk_id = chunks.id),
          analyzer_model = ?,
          enriched_at = (SELECT s.enriched_at FROM staged_chunk_analysis s WHERE s.generation_id = ? AND s.chunk_id = chunks.id)
        WHERE id IN (SELECT chunk_id FROM staged_chunk_analysis WHERE generation_id = ?)
        """) { statement in
        bindText(statement, 1, generationId)
        bindText(statement, 2, generationId)
        bindText(statement, 3, generationId)
        bindText(statement, 4, generation.analyzerModel)
        bindText(statement, 5, generationId)
        bindText(statement, 6, generationId)
      }

      try execute(sql: """
        INSERT OR REPLACE INTO chunk_analysis (
          chunk_id, analyzer_model, ai_summary, ai_tags, analyzed_at, enriched_at, source
        )
        SELECT chunk_id, ?, ai_summary, ai_tags, analyzed_at, enriched_at, 'local'
        FROM staged_chunk_analysis WHERE generation_id = ?
        """) { statement in
        bindText(statement, 1, generation.analyzerModel)
        bindText(statement, 2, generationId)
      }

      try execute(sql: """
        INSERT OR REPLACE INTO embeddings (chunk_id, embedding)
        SELECT chunk_id, embedding FROM staged_chunk_analysis WHERE generation_id = ?
        """) { bindText($0, 1, generationId) }

      if extensionLoaded {
        try execute(sql: "DELETE FROM vec_chunks WHERE repo_id = ?") {
          bindText($0, 1, generation.repoId)
        }
        try execute(sql: """
          INSERT INTO vec_chunks (chunk_id, repo_id, embedding)
          SELECT s.chunk_id, f.repo_id, s.embedding
          FROM staged_chunk_analysis s
          JOIN chunks c ON c.id = s.chunk_id
          JOIN files f ON f.id = c.file_id
          WHERE s.generation_id = ? AND f.repo_id = ?
          """) { statement in
          bindText(statement, 1, generationId)
          bindText(statement, 2, generation.repoId)
        }
      }

      try execute(sql: """
        INSERT OR REPLACE INTO ai_summary_cache (
          text_hash, ai_summary, ai_tags, analyzer_model, cached_at
        )
        SELECT text_hash, ai_summary, COALESCE(ai_tags, '[]'), ?, analyzed_at
        FROM staged_chunk_analysis WHERE generation_id = ?
        """) { statement in
        bindText(statement, 1, generation.analyzerModel)
        bindText(statement, 2, generationId)
      }

      try execute(sql: """
        UPDATE repos SET analyzer_model = ?, active_analysis_generation = ?
        WHERE id = ?
        """) { statement in
        bindText(statement, 1, generation.analyzerModel)
        bindText(statement, 2, generationId)
        bindText(statement, 3, generation.repoId)
      }
      let committedAt = dateFormatter.string(from: Date())
      try execute(sql: """
        UPDATE analysis_generations SET state = 'committed', committed_at = ?
        WHERE id = ?
        """) { statement in
        bindText(statement, 1, committedAt)
        bindText(statement, 2, generationId)
      }
      try execute(sql: "DELETE FROM staged_chunk_analysis WHERE generation_id = ?") {
        bindText($0, 1, generationId)
      }
      try execute(sql: """
        DELETE FROM analysis_generations
        WHERE repo_id = ? AND state = 'committed' AND id != ?
        """) { statement in
        bindText(statement, 1, generation.repoId)
        bindText(statement, 2, generationId)
      }
      try exec("COMMIT")
    } catch {
      try? exec("ROLLBACK")
      throw error
    }

    return try analysisGenerationStatus(generationId: generationId)
  }

  /// Drop an uncommitted replacement generation. Active search state is never
  /// touched, so abort is safe before or after partial analysis/enrichment.
  public func abortAnalysisGeneration(generationId: String) throws {
    try openIfNeeded()
    try ensureSchema()
    let generation = try loadAnalysisGeneration(generationId: generationId)
    guard generation.phase != .committed else {
      throw RAGError.sqlite("Committed analysis generation \(generationId) cannot be aborted")
    }
    do {
      try exec("BEGIN IMMEDIATE TRANSACTION")
      try execute(sql: "DELETE FROM staged_chunk_analysis WHERE generation_id = ?") {
        bindText($0, 1, generationId)
      }
      try execute(sql: "DELETE FROM analysis_generations WHERE id = ?") {
        bindText($0, 1, generationId)
      }
      try exec("COMMIT")
    } catch {
      try? exec("ROLLBACK")
      throw error
    }
  }

  // MARK: - Generation bookkeeping

  private func uncommittedAnalysisGeneration(
    repoId: String
  ) throws -> AnalysisGenerationRecord? {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = """
      SELECT id FROM analysis_generations
      WHERE repo_id = ? AND state IN ('analyzing', 'ready')
      ORDER BY created_at DESC LIMIT 1
      """
    var statement: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK,
          let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }
    bindText(statement, 1, repoId)
    guard sqlite3_step(statement) == SQLITE_ROW,
          let raw = sqlite3_column_text(statement, 0) else { return nil }
    return try loadAnalysisGeneration(generationId: String(cString: raw))
  }

  private func loadAnalysisGeneration(
    generationId: String
  ) throws -> AnalysisGenerationRecord {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = """
      SELECT id, repo_id, analyzer_model, state, created_at, committed_at
      FROM analysis_generations WHERE id = ?
      """
    var statement: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK,
          let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }
    bindText(statement, 1, generationId)
    guard sqlite3_step(statement) == SQLITE_ROW else {
      throw RAGError.sqlite("No analysis generation matches \(generationId)")
    }
    let phaseRaw = String(cString: sqlite3_column_text(statement, 3))
    guard let phase = AnalysisGenerationPhase(rawValue: phaseRaw) else {
      throw RAGError.sqlite("Analysis generation \(generationId) has invalid state \(phaseRaw)")
    }
    let createdRaw = String(cString: sqlite3_column_text(statement, 4))
    guard let createdAt = dateFormatter.date(from: createdRaw) else {
      throw RAGError.sqlite("Analysis generation \(generationId) has invalid creation date")
    }
    let committedAt = sqlite3_column_text(statement, 5)
      .map { String(cString: $0) }
      .flatMap { dateFormatter.date(from: $0) }
    return AnalysisGenerationRecord(
      id: String(cString: sqlite3_column_text(statement, 0)),
      repoId: String(cString: sqlite3_column_text(statement, 1)),
      analyzerModel: String(cString: sqlite3_column_text(statement, 2)),
      phase: phase,
      createdAt: createdAt,
      committedAt: committedAt
    )
  }

  private func analysisGenerationStatus(
    record: AnalysisGenerationRecord
  ) throws -> AnalysisGenerationStatus {
    let total = try queryInt("""
      SELECT COUNT(*) FROM chunks c JOIN files f ON f.id = c.file_id
      WHERE f.repo_id = ?
      """) { bindText($0, 1, record.repoId) }

    let analyzed: Int
    let enriched: Int
    if record.phase == .committed {
      analyzed = try queryInt("""
        SELECT COUNT(*) FROM chunks c JOIN files f ON f.id = c.file_id
        JOIN repos r ON r.id = f.repo_id
        WHERE f.repo_id = ? AND r.active_analysis_generation = ?
          AND c.ai_summary IS NOT NULL AND c.analyzer_model = ?
        """) { statement in
        bindText(statement, 1, record.repoId)
        bindText(statement, 2, record.id)
        bindText(statement, 3, record.analyzerModel)
      }
      enriched = try queryInt("""
        SELECT COUNT(*) FROM chunks c JOIN files f ON f.id = c.file_id
        JOIN repos r ON r.id = f.repo_id
        WHERE f.repo_id = ? AND r.active_analysis_generation = ?
          AND c.enriched_at IS NOT NULL AND c.analyzer_model = ?
        """) { statement in
        bindText(statement, 1, record.repoId)
        bindText(statement, 2, record.id)
        bindText(statement, 3, record.analyzerModel)
      }
    } else {
      analyzed = try queryInt("""
        SELECT COUNT(*) FROM staged_chunk_analysis s
        JOIN chunks c ON c.id = s.chunk_id
        JOIN files f ON f.id = c.file_id
        WHERE s.generation_id = ? AND f.repo_id = ?
        """) { statement in
        bindText(statement, 1, record.id)
        bindText(statement, 2, record.repoId)
      }
      enriched = try queryInt("""
        SELECT COUNT(*) FROM staged_chunk_analysis s
        JOIN chunks c ON c.id = s.chunk_id
        JOIN files f ON f.id = c.file_id
        WHERE s.generation_id = ? AND f.repo_id = ? AND s.embedding IS NOT NULL
        """) { statement in
        bindText(statement, 1, record.id)
        bindText(statement, 2, record.repoId)
      }
    }

    return AnalysisGenerationStatus(
      generationId: record.id,
      repoId: record.repoId,
      analyzerModel: record.analyzerModel,
      phase: record.phase,
      createdAt: record.createdAt,
      committedAt: record.committedAt,
      totalChunks: total,
      analyzedChunks: analyzed,
      enrichedChunks: enriched
    )
  }
}
