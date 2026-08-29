//
//  RAGStore+TextIndex.swift
//  RAGCore
//
//  The FTS5 lexical index behind text search (cloke/peel#2211): schema,
//  trigger maintenance, backfill, coverage tracking, and the BM25 query.
//
//  Shape: a contentless-delete FTS5 table keyed by chunks.rowid. Nothing is
//  stored twice - the index holds postings only, and result text comes from
//  the chunks table via the rowid join. Maintenance is triggers on `chunks`
//  calling the registered `code_tokens()` SQL function, so every write path
//  (indexing, analysis promotion, pruning, imports) keeps the index in sync
//  without knowing it exists. `PRAGMA recursive_triggers=ON` makes
//  INSERT OR REPLACE fire the delete trigger for the row it replaces.
//
//  Coverage is tracked per repo in fts_repo_state, because the v23 migration
//  deliberately does NOT scan the chunks table at open (the v21 precedent: a
//  whole-table migration turns the first launch after upgrade into a stall).
//  A repo serves BM25 once its backfill ran - via indexRepository, an
//  explicit rebuildTextIndex, or the one-time coverage verification - and
//  serves the legacy substring path, flagged, until then.
//

import CSQLite
import Foundation

/// What one text search actually did: which ranking served, and whether any
/// in-scope repo is still waiting for its lexical index backfill. Substring
/// with `pendingTextIndexRepos > 0` is the degraded shape; callers surface
/// it rather than presenting alphabetical truncation as ranking.
public struct RAGTextSearchOutcome: Sendable {
  public enum Ranking: String, Sendable {
    case bm25
    case substring
  }

  public let results: [RAGSearchResult]
  public let ranking: Ranking
  /// Repos in scope whose chunks are not yet fully covered by the FTS index.
  /// 0 when BM25 served; positive when substring served because coverage is
  /// incomplete.
  public let pendingTextIndexRepos: Int

  public init(results: [RAGSearchResult], ranking: Ranking, pendingTextIndexRepos: Int) {
    self.results = results
    self.ranking = ranking
    self.pendingTextIndexRepos = pendingTextIndexRepos
  }
}

extension RAGStore {

  // MARK: - Connection setup

  /// Register the `code_tokens(x)` scalar used by the FTS triggers and the
  /// backfill. Must run before any statement that can fire those triggers.
  ///
  /// PUBLIC ON PURPOSE, and load-bearing for out-of-process writers: the
  /// triggers are persistent schema objects, so EVERY connection that
  /// inserts into `chunks` — or updates its text, construct_name,
  /// ai_summary, or file_id — must have this function registered, or SQLite
  /// fails the write with "no such function: code_tokens". A host app that
  /// opens raw connections to the store file for writing (Peel's overlay
  /// sync, secret scrubbing, and quality scanning do) must call this on each
  /// such connection right after opening it. Read-only connections and
  /// deletes never need it.
  public static func registerCodeTokens(on handle: OpaquePointer) throws {
    let flags = SQLITE_UTF8 | SQLITE_DETERMINISTIC
    let rc = sqlite3_create_function_v2(
      handle, "code_tokens", 1, flags, nil,
      { context, argumentCount, arguments in
        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        guard argumentCount == 1,
              let value = arguments?.pointee,
              let cString = sqlite3_value_text(value) else {
          sqlite3_result_text(context, "", -1, transient)
          return
        }
        let transformed = CodeTokens.indexText(String(cString: cString))
        sqlite3_result_text(context, transformed, -1, transient)
      },
      nil, nil, nil
    )
    guard rc == SQLITE_OK else {
      throw RAGError.sqlite("Cannot register code_tokens function: \(rc)")
    }
  }

  // MARK: - Schema (called from the v23 migration)

  internal func createTextIndexSchema() throws {
    try exec("""
      CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
        text, construct_name, ai_summary, path,
        content='', contentless_delete=1,
        tokenize='unicode61'
      )
      """)
    try exec("""
      CREATE TABLE IF NOT EXISTS fts_repo_state (
        repo_id TEXT PRIMARY KEY,
        complete INTEGER NOT NULL DEFAULT 0
      )
      """)
    // The insert trigger deletes first so a REPLACE that keeps its rowid, or
    // a re-fired trigger, can never double-insert a posting set.
    try exec("""
      CREATE TRIGGER IF NOT EXISTS chunks_fts_after_insert AFTER INSERT ON chunks BEGIN
        DELETE FROM chunks_fts WHERE rowid = new.rowid;
        INSERT INTO chunks_fts(rowid, text, construct_name, ai_summary, path)
        VALUES (
          new.rowid,
          code_tokens(new.text),
          code_tokens(COALESCE(new.construct_name, '')),
          COALESCE(new.ai_summary, ''),
          code_tokens(COALESCE((SELECT path FROM files WHERE id = new.file_id), ''))
        );
      END
      """)
    try exec("""
      CREATE TRIGGER IF NOT EXISTS chunks_fts_after_delete AFTER DELETE ON chunks BEGIN
        DELETE FROM chunks_fts WHERE rowid = old.rowid;
      END
      """)
    try exec("""
      CREATE TRIGGER IF NOT EXISTS chunks_fts_after_update
      AFTER UPDATE OF text, construct_name, ai_summary, file_id ON chunks BEGIN
        DELETE FROM chunks_fts WHERE rowid = old.rowid;
        INSERT INTO chunks_fts(rowid, text, construct_name, ai_summary, path)
        VALUES (
          new.rowid,
          code_tokens(new.text),
          code_tokens(COALESCE(new.construct_name, '')),
          COALESCE(new.ai_summary, ''),
          code_tokens(COALESCE((SELECT path FROM files WHERE id = new.file_id), ''))
        );
      END
      """)
  }

  // MARK: - Coverage

  /// True when every repo in scope has a complete FTS index. A repo with no
  /// recorded state gets one cheap verification pass (point lookups against
  /// the index), cached in fts_repo_state, so a store whose chunks all
  /// arrived through the triggers self-heals to complete without a rebuild.
  internal func textIndexComplete(resolvedRepoId: String?) throws -> Int {
    let repoIds: [String]
    if let resolvedRepoId {
      repoIds = [resolvedRepoId]
    } else {
      repoIds = try queryStrings("SELECT id FROM repos")
    }
    var pending = 0
    for repoId in repoIds {
      let state = try queryInt(
        "SELECT complete FROM fts_repo_state WHERE repo_id = ?"
      ) { stmt in
        bindText(stmt, 1, repoId)
      }
      if state == 1 { continue }
      let hasState = try queryInt(
        "SELECT COUNT(*) FROM fts_repo_state WHERE repo_id = ?"
      ) { stmt in
        bindText(stmt, 1, repoId)
      }
      if hasState == 0, try verifyTextIndexCoverage(repoId: repoId) { continue }
      pending += 1
    }
    return pending
  }

  /// Count the repo's chunks missing from the index; record the verdict.
  /// Runs once per repo (the verdict is cached either way), so the point
  /// lookups are a one-time cost, not a per-search one.
  internal func verifyTextIndexCoverage(repoId: String) throws -> Bool {
    let missing = try queryInt("""
      SELECT COUNT(*)
      FROM chunks
      JOIN files ON files.id = chunks.file_id
      WHERE files.repo_id = ?
        AND NOT EXISTS (SELECT 1 FROM chunks_fts WHERE chunks_fts.rowid = chunks.rowid)
      """) { stmt in
      bindText(stmt, 1, repoId)
    }
    let complete = missing == 0
    try execute(sql: """
      INSERT INTO fts_repo_state (repo_id, complete) VALUES (?, ?)
      ON CONFLICT(repo_id) DO UPDATE SET complete = excluded.complete
      """) { stmt in
      bindText(stmt, 1, repoId)
      sqlite3_bind_int(stmt, 2, complete ? 1 : 0)
    }
    return complete
  }

  internal func markTextIndexComplete(repoId: String) throws {
    try execute(sql: """
      INSERT INTO fts_repo_state (repo_id, complete) VALUES (?, 1)
      ON CONFLICT(repo_id) DO UPDATE SET complete = 1
      """) { stmt in
      bindText(stmt, 1, repoId)
    }
  }

  // MARK: - Backfill

  /// Index every chunk the FTS table does not cover yet, for one repo or the
  /// whole store, and record completeness. Returns the number of chunks
  /// backfilled. Runs inside the calling task on the store actor; the cost is
  /// proportional to the UNCOVERED rows only, so a store that stays current
  /// pays nothing.
  @discardableResult
  public func rebuildTextIndex(repoPath: String? = nil) async throws -> Int {
    try openIfNeeded()
    try ensureSchema()
    let resolvedRepoId: String?
    if let repoPath {
      guard let repo = try resolveRepo(for: repoPath) else { return 0 }
      resolvedRepoId = repo.id
    } else {
      resolvedRepoId = nil
    }
    return try backfillTextIndex(resolvedRepoId: resolvedRepoId)
  }

  @discardableResult
  internal func backfillTextIndex(resolvedRepoId: String?) throws -> Int {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var sql = """
      INSERT INTO chunks_fts(rowid, text, construct_name, ai_summary, path)
      SELECT chunks.rowid,
             code_tokens(chunks.text),
             code_tokens(COALESCE(chunks.construct_name, '')),
             COALESCE(chunks.ai_summary, ''),
             code_tokens(COALESCE(files.path, ''))
      FROM chunks
      JOIN files ON files.id = chunks.file_id
      WHERE NOT EXISTS (SELECT 1 FROM chunks_fts WHERE chunks_fts.rowid = chunks.rowid)
      """
    if resolvedRepoId != nil {
      sql += " AND files.repo_id = ?"
    }
    try execute(sql: sql) { stmt in
      if let resolvedRepoId {
        bindText(stmt, 1, resolvedRepoId)
      }
    }
    let backfilled = Int(sqlite3_changes(db))
    let repoIds = try resolvedRepoId.map { [$0] } ?? queryStrings("SELECT id FROM repos")
    for repoId in repoIds {
      try markTextIndexComplete(repoId: repoId)
    }
    return backfilled
  }

  // MARK: - BM25 query

  /// Per-column BM25 weights: construct name and path outweigh body text,
  /// summaries sit between. Order matches the chunks_fts column order
  /// (text, construct_name, ai_summary, path).
  internal static let bm25Weights = "1.0, 4.0, 2.0, 3.0"

  internal func searchBM25(
    matchExpression: String,
    resolvedRepoId: String?,
    limit: Int,
    modulePath: String?
  ) throws -> [RAGSearchResult] {
    // Repo and module filters apply after the rowid join, so the MATCH window
    // must be wider than the limit or a scoped search could starve on hits
    // that belong to other repos. One escalation retry covers the pathological
    // case (a common term dominated by out-of-scope repos) without making
    // every query pay for it.
    let filtered = resolvedRepoId != nil || modulePath != nil
    let firstWindow = filtered ? max(limit * 20, 400) : max(limit, 1)
    let results = try runBM25Query(
      matchExpression: matchExpression, resolvedRepoId: resolvedRepoId,
      limit: limit, modulePath: modulePath, window: firstWindow
    )
    if filtered, results.count < limit {
      let escalated = try runBM25Query(
        matchExpression: matchExpression, resolvedRepoId: resolvedRepoId,
        limit: limit, modulePath: modulePath, window: max(firstWindow * 25, 10_000)
      )
      if escalated.count > results.count { return escalated }
    }
    return results
  }

  private func runBM25Query(
    matchExpression: String,
    resolvedRepoId: String?,
    limit: Int,
    modulePath: String?,
    window: Int
  ) throws -> [RAGSearchResult] {
    var sql = """
      WITH matched AS (
        SELECT rowid, bm25(chunks_fts, \(Self.bm25Weights)) AS rank_score
        FROM chunks_fts
        WHERE chunks_fts MATCH ?
        ORDER BY rank_score
        LIMIT ?
      )
      SELECT repos.root_path || '/' || files.path, chunks.start_line, chunks.end_line, chunks.text,
             chunks.construct_type, chunks.construct_name, files.language, files.module_path, files.feature_tags,
             chunks.ai_summary, chunks.ai_tags, chunks.token_count,
             chunks.comment_lines, chunks.code_lines, chunks.max_comment_block, chunks.has_commented_out_code,
             matched.rank_score
      FROM matched
      JOIN chunks ON chunks.rowid = matched.rowid
      JOIN files ON files.id = chunks.file_id
      JOIN repos ON repos.id = files.repo_id
      WHERE 1=1
      """
    if resolvedRepoId != nil { sql += " AND repos.id = ?" }
    if modulePath != nil { sql += " AND LOWER(files.module_path) LIKE ?" }
    sql += " ORDER BY matched.rank_score LIMIT ?"

    return try querySearchResults(sql: sql, withScore: true, scoreColumn: 16) { stmt in
      var bindIndex: Int32 = 1
      bindText(stmt, bindIndex, matchExpression)
      bindIndex += 1
      sqlite3_bind_int(stmt, bindIndex, Int32(max(1, window)))
      bindIndex += 1
      if let resolvedRepoId {
        bindText(stmt, bindIndex, resolvedRepoId)
        bindIndex += 1
      }
      if let modulePath {
        bindText(stmt, bindIndex, "%\(modulePath.lowercased())%")
        bindIndex += 1
      }
      sqlite3_bind_int(stmt, bindIndex, Int32(max(1, limit)))
    }
  }

  // MARK: - Small helpers

  internal func queryStrings(_ sql: String) throws -> [String] {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }
    var values: [String] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      if let text = sqlite3_column_text(stmt, 0) {
        values.append(String(cString: text))
      }
    }
    return values
  }
}
