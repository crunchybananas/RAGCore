//
//  RAGStore+Structure.swift
//  RAGCore
//
//  Structural comment metrics: backfill and queries (cloke/peel#2202).
//

import CSQLite
import Foundation

/// Outcome of a structure-metric backfill pass.
public struct StructureBackfillReport: Sendable {
  /// Chunks whose metrics were computed and written.
  public let measured: Int
  /// Chunks visited whose language has no comment syntax, so they stay NULL.
  public let unmeasurable: Int
  /// Chunks still missing metrics after this pass — non-zero when `limit`
  /// stopped it short.
  public let remaining: Int

  public var isComplete: Bool { remaining == 0 }

  public init(measured: Int, unmeasurable: Int, remaining: Int) {
    self.measured = measured
    self.unmeasurable = unmeasurable
    self.remaining = remaining
  }
}

/// A chunk that matched a structural query.
public struct StructuralChunkMatch: Sendable {
  public let filePath: String
  public let relativePath: String
  public let startLine: Int
  public let endLine: Int
  public let language: String?
  public let constructName: String?
  public let metrics: ChunkStructureMetrics

  public init(
    filePath: String, relativePath: String, startLine: Int, endLine: Int,
    language: String?, constructName: String?, metrics: ChunkStructureMetrics
  ) {
    self.filePath = filePath
    self.relativePath = relativePath
    self.startLine = startLine
    self.endLine = endLine
    self.language = language
    self.constructName = constructName
    self.metrics = metrics
  }
}

/// Results of a structural query, with the unmeasured population named.
///
/// A structural filter over a partially-backfilled index would otherwise return
/// a short list that looks complete. `unmeasuredChunks` is how a caller knows
/// the answer is bounded by what has been measured, not by what exists.
public struct StructuralQueryResult: Sendable {
  public let matches: [StructuralChunkMatch]
  /// Chunks in scope whose metrics are NULL — not searched, not "no match".
  public let unmeasuredChunks: Int

  public var isComplete: Bool { unmeasuredChunks == 0 }

  public init(matches: [StructuralChunkMatch], unmeasuredChunks: Int) {
    self.matches = matches
    self.unmeasuredChunks = unmeasuredChunks
  }
}

extension RAGStore {

  // MARK: - Row decoding

  /// Read the four structure columns starting at `firstColumn`, or nil when the
  /// row predates schema v19 / its language has no comment syntax.
  ///
  /// Shared by all three search paths so a NULL is interpreted one way. Reading
  /// `sqlite3_column_int` on a NULL yields 0, which would quietly turn every
  /// unmeasured chunk into a fully-measured chunk with no comments — the exact
  /// lie the nil-vs-zero distinction exists to prevent.
  func decodeStructureMetrics(
    _ stmt: OpaquePointer, firstColumn: Int32
  ) -> ChunkStructureMetrics? {
    guard sqlite3_column_type(stmt, firstColumn) != SQLITE_NULL else { return nil }
    return ChunkStructureMetrics(
      commentLines: Int(sqlite3_column_int(stmt, firstColumn)),
      codeLines: Int(sqlite3_column_int(stmt, firstColumn + 1)),
      maxCommentBlockLines: Int(sqlite3_column_int(stmt, firstColumn + 2)),
      containsCommentedOutCode: sqlite3_column_int(stmt, firstColumn + 3) == 1
    )
  }

  // MARK: - Backfill

  /// Compute structure metrics for chunks indexed before schema v19.
  ///
  /// The v19 migration adds the columns but leaves them NULL: filling them needs
  /// every chunk's text plus its file's language, and doing that inside `open()`
  /// would stall the first launch after an upgrade on a large index. This is the
  /// deliberate, callable version of that work.
  ///
  /// - Parameter limit: maximum chunks to measure in this pass. The report's
  ///   `remaining` says whether another pass is needed, so a host can spread the
  ///   work instead of blocking on it.
  @discardableResult
  public func backfillStructureMetrics(
    repoPath: String? = nil,
    limit: Int = 5_000
  ) throws -> StructureBackfillReport {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let resolvedRepoId: String? = if let repoPath { try resolveRepoId(for: repoPath) } else { nil }

    var sql = """
      SELECT c.id, c.text, f.language
      FROM chunks c JOIN files f ON c.file_id = f.id
      """
    if resolvedRepoId != nil { sql += " JOIN repos r ON f.repo_id = r.id" }
    sql += " WHERE c.comment_lines IS NULL"
    if resolvedRepoId != nil { sql += " AND r.id = ?" }
    sql += " LIMIT ?"

    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    var pending: [(id: String, metrics: ChunkStructureMetrics?)] = []
    do {
      defer { sqlite3_finalize(stmt) }
      if let resolvedRepoId {
        bindText(stmt, 1, resolvedRepoId)
        sqlite3_bind_int(stmt, 2, Int32(limit))
      } else {
        sqlite3_bind_int(stmt, 1, Int32(limit))
      }
      while sqlite3_step(stmt) == SQLITE_ROW {
        let id = String(cString: sqlite3_column_text(stmt, 0))
        let text = String(cString: sqlite3_column_text(stmt, 1))
        let language: String? = sqlite3_column_type(stmt, 2) != SQLITE_NULL
          ? String(cString: sqlite3_column_text(stmt, 2)) : nil
        pending.append((id, ChunkStructureMetrics.compute(text: text, language: language)))
      }
    }

    var measured = 0
    var unmeasurable = 0
    for row in pending {
      guard let metrics = row.metrics else {
        // No comment syntax for this language. The columns stay NULL, so the
        // row is skipped again on the next pass rather than being mistaken for
        // measured-and-zero. `unmeasurable` is what keeps that honest.
        unmeasurable += 1
        continue
      }
      try execute(sql: """
        UPDATE chunks SET comment_lines = ?, code_lines = ?, max_comment_block = ?,
                          has_commented_out_code = ?
        WHERE id = ?
        """) { update in
        sqlite3_bind_int(update, 1, Int32(metrics.commentLines))
        sqlite3_bind_int(update, 2, Int32(metrics.codeLines))
        sqlite3_bind_int(update, 3, Int32(metrics.maxCommentBlockLines))
        sqlite3_bind_int(update, 4, metrics.containsCommentedOutCode ? 1 : 0)
        bindText(update, 5, row.id)
      }
      measured += 1
    }

    let remaining = try unmeasuredChunkCount(repoId: resolvedRepoId) - unmeasurable
    return StructureBackfillReport(
      measured: measured,
      unmeasurable: unmeasurable,
      remaining: max(0, remaining)
    )
  }

  /// Reset every chunk's structure metrics to NULL.
  ///
  /// Exists so tests can reproduce a pre-v19 index — the state the backfill and
  /// the unmeasured-count reporting are built for — without shipping a fixture
  /// database that would need regenerating on every schema bump.
  public func clearStructureMetricsForTesting() throws {
    try openIfNeeded()
    try exec("""
      UPDATE chunks SET comment_lines = NULL, code_lines = NULL,
                        max_comment_block = NULL, has_commented_out_code = NULL
      """)
  }

  /// Chunks whose structure metrics are NULL.
  func unmeasuredChunkCount(repoId: String?) throws -> Int {
    var sql = "SELECT COUNT(*) FROM chunks c JOIN files f ON c.file_id = f.id"
    if repoId != nil { sql += " JOIN repos r ON f.repo_id = r.id" }
    sql += " WHERE c.comment_lines IS NULL"
    if repoId != nil { sql += " AND r.id = ?" }
    if let repoId {
      return try queryInt(sql, bind: { stmt in bindText(stmt, 1, repoId) })
    }
    return try queryInt(sql)
  }

  // MARK: - Structural queries

  /// Chunks matching structural comment criteria.
  ///
  /// This is the query retrieval cannot express: not "code about cleanup" but
  /// "code that *is* a cleanup candidate". All filters are ANDed, and any left
  /// nil is not applied.
  ///
  /// - Parameters:
  ///   - minCommentRatio: comment lines / non-blank lines, 0...1.
  ///   - requireCommentedOutCode: only chunks whose comments contain code.
  ///   - minCommentBlockLines: only chunks with a comment run at least this long.
  ///   - minLines: ignore chunks smaller than this, so a two-line chunk with one
  ///     comment cannot top a ratio ranking.
  public func findStructuralChunks(
    repoPath: String? = nil,
    minCommentRatio: Double? = nil,
    requireCommentedOutCode: Bool = false,
    minCommentBlockLines: Int? = nil,
    minLines: Int = 0,
    limit: Int = 50
  ) throws -> StructuralQueryResult {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let resolvedRepoId: String? = if let repoPath { try resolveRepoId(for: repoPath) } else { nil }

    var conditions = ["c.comment_lines IS NOT NULL"]
    if resolvedRepoId != nil { conditions.append("r.id = ?") }
    if requireCommentedOutCode { conditions.append("c.has_commented_out_code = 1") }
    if minCommentBlockLines != nil { conditions.append("c.max_comment_block >= ?") }
    if minLines > 0 { conditions.append("(c.end_line - c.start_line + 1) >= ?") }
    if minCommentRatio != nil {
      // Ratio is derived, never stored, so it cannot drift from its inputs.
      // The denominator guard keeps a comment-only chunk from dividing by zero.
      conditions.append("(c.comment_lines + c.code_lines) > 0")
      conditions.append(
        "CAST(c.comment_lines AS REAL) / (c.comment_lines + c.code_lines) >= ?")
    }

    let sql = """
      SELECT r.root_path || '/' || f.path, f.path, c.start_line, c.end_line,
             f.language, c.construct_name,
             c.comment_lines, c.code_lines, c.max_comment_block, c.has_commented_out_code
      FROM chunks c
      JOIN files f ON c.file_id = f.id
      JOIN repos r ON f.repo_id = r.id
      WHERE \(conditions.joined(separator: " AND "))
      ORDER BY CAST(c.comment_lines AS REAL) / MAX(c.comment_lines + c.code_lines, 1) DESC,
               c.max_comment_block DESC
      LIMIT ?
      """

    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }

    var position: Int32 = 1
    if let resolvedRepoId { bindText(stmt, position, resolvedRepoId); position += 1 }
    if let minCommentBlockLines {
      sqlite3_bind_int(stmt, position, Int32(minCommentBlockLines)); position += 1
    }
    if minLines > 0 { sqlite3_bind_int(stmt, position, Int32(minLines)); position += 1 }
    if let minCommentRatio {
      sqlite3_bind_double(stmt, position, minCommentRatio); position += 1
    }
    sqlite3_bind_int(stmt, position, Int32(limit))

    var matches: [StructuralChunkMatch] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let language: String? = sqlite3_column_type(stmt, 4) != SQLITE_NULL
        ? String(cString: sqlite3_column_text(stmt, 4)) : nil
      let constructName: String? = sqlite3_column_type(stmt, 5) != SQLITE_NULL
        ? String(cString: sqlite3_column_text(stmt, 5)) : nil
      matches.append(StructuralChunkMatch(
        filePath: String(cString: sqlite3_column_text(stmt, 0)),
        relativePath: String(cString: sqlite3_column_text(stmt, 1)),
        startLine: Int(sqlite3_column_int(stmt, 2)),
        endLine: Int(sqlite3_column_int(stmt, 3)),
        language: language,
        constructName: constructName,
        metrics: ChunkStructureMetrics(
          commentLines: Int(sqlite3_column_int(stmt, 6)),
          codeLines: Int(sqlite3_column_int(stmt, 7)),
          maxCommentBlockLines: Int(sqlite3_column_int(stmt, 8)),
          containsCommentedOutCode: sqlite3_column_int(stmt, 9) == 1
        )
      ))
    }

    return StructuralQueryResult(
      matches: matches,
      unmeasuredChunks: try unmeasuredChunkCount(repoId: resolvedRepoId)
    )
  }
}
