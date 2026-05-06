//
//  RAGStore+Symbols.swift
//  RAGCore
//
//  Symbol reference lookups, backed by the AST chunker's `symbol_refs`
//  index. Used by callers that need to answer "who references X?" — the
//  shape of query refactor workflows hit constantly. The chunker already
//  populates this index per file (Swift, Ruby, TypeScript, JavaScript,
//  Glimmer gts/gjs); this extension just exposes a clean read API rather
//  than forcing each caller to wrangle the SQLite tables themselves.
//
//  Companion to RAGStore+Dependencies (file-level imports) — that one
//  answers "which files does file A import?", this one answers "which
//  files mention symbol X?". Together they form the read side of the
//  refactor symbol graph.
//

import CSQLite
import Foundation

// MARK: - Result Types

/// One match from `findSymbolReferences`. Keeps both the repo's stable
/// identifier (for cross-machine equality checks) and the human-readable
/// `repoName` so consumers can render results without an extra round trip.
public struct RAGSymbolReferenceResult: Sendable {
  /// Stable repo identifier (e.g. "github.com/cloke/peel"). May be empty
  /// for repos registered before the identifier column was populated.
  public let repoIdentifier: String
  /// Display name from `repos.name` (e.g. "peel").
  public let repoName: String
  /// Absolute path to the repo root on this machine. Combine with
  /// `relativePath` to get the file's full path; we keep them separate
  /// so callers that don't want to assume on-disk layout can ignore it.
  public let repoRootPath: String
  /// Path within the repo (e.g. "macOS/Applications/SwarmDashboardView.swift").
  public let relativePath: String
  /// Source language as detected by the chunker (e.g. "Swift", "Ruby",
  /// "TypeScript"). Optional because legacy rows may lack this column.
  public let language: String?
  /// Reference kind from the chunker (typically "type"; future kinds may
  /// distinguish call vs. import vs. extension references).
  public let refKind: String

  public init(
    repoIdentifier: String,
    repoName: String,
    repoRootPath: String,
    relativePath: String,
    language: String?,
    refKind: String
  ) {
    self.repoIdentifier = repoIdentifier
    self.repoName = repoName
    self.repoRootPath = repoRootPath
    self.relativePath = relativePath
    self.language = language
    self.refKind = refKind
  }
}

// MARK: - Queries

extension RAGStore {

  /// Find every file that references a named symbol.
  ///
  /// - Parameters:
  ///   - name: Exact symbol name to look up (case-sensitive). The chunker
  ///     stores symbols verbatim (e.g. "Component", "RAGStore"), so this
  ///     should be the canonical identifier — substring matching is not
  ///     supported by the underlying index.
  ///   - repoPath: Optional repo path filter. When supplied, results are
  ///     scoped to that repo via `resolveRepo(for:)` (so callers can pass
  ///     either an absolute path or anything else `resolveRepo` accepts).
  ///   - limit: Hard cap on rows returned. Defaults to 100; popular type
  ///     names (e.g. "Component", "Application") commonly have hundreds
  ///     of hits across a swarm.
  ///   - refKind: Optional filter on the chunker's reference kind. The
  ///     chunker emits `"type"` for general type usage, `"conform"` for
  ///     protocol conformances, `"inherit"` for class inheritance, and
  ///     `"mixin"` for include/extend/prepend. Callers asking refactor
  ///     questions (e.g. "who CONFORMS to this protocol?") should pass
  ///     the matching kind to avoid drowning in plain type-usage hits.
  ///     Pass `nil` (default) to match every kind.
  ///   - language: Optional source-language filter (case-insensitive
  ///     match against the chunker's `f.language` column, e.g. `"Swift"`,
  ///     `"Ruby"`, `"TypeScript"`). Useful when the same symbol name
  ///     appears across stacks — Ember `Component` references shouldn't
  ///     drown out Swift `Component` references during a Swift refactor.
  ///     Pass `nil` (default) to match every language.
  /// - Returns: Reference rows ordered by repo name then file path so
  ///   results within the same repo group naturally.
  public func findSymbolReferences(
    name: String,
    repoPath: String? = nil,
    limit: Int = 100,
    refKind: String? = nil,
    language: String? = nil
  ) throws -> [RAGSymbolReferenceResult] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    // If the caller scoped to a repo, resolve it the same way the rest
    // of the queries do (resolveRepo handles repo_identifier remapping).
    // Returning [] for an unknown repo is consistent with findOrphans.
    let scopedRepoId: String?
    if let repoPath {
      guard let resolved = try resolveRepo(for: repoPath) else { return [] }
      scopedRepoId = resolved.id
    } else {
      scopedRepoId = nil
    }

    var sql = """
      SELECT r.repo_identifier, r.name, r.root_path, f.path, f.language, sr.ref_kind
      FROM symbol_refs sr
      JOIN files f ON f.id = sr.source_file_id
      JOIN repos r ON r.id = sr.repo_id
      WHERE sr.referenced_name = ?
      """
    if scopedRepoId != nil {
      sql += " AND sr.repo_id = ?"
    }
    if refKind != nil {
      sql += " AND sr.ref_kind = ?"
    }
    if language != nil {
      // Case-insensitive match — chunker stores "Swift", but callers
      // passing "swift" shouldn't get an empty result for that.
      sql += " AND LOWER(f.language) = LOWER(?)"
    }
    sql += " ORDER BY r.name, f.path LIMIT ?"

    var stmt: OpaquePointer?
    let prepResult = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard prepResult == SQLITE_OK, let stmt else {
      let errMsg = String(cString: sqlite3_errmsg(db))
      throw RAGError.sqlite("findSymbolReferences prepare failed: \(errMsg)")
    }
    defer { sqlite3_finalize(stmt) }

    var bindIdx: Int32 = 1
    bindText(stmt, bindIdx, name); bindIdx += 1
    if let scopedRepoId {
      bindText(stmt, bindIdx, scopedRepoId); bindIdx += 1
    }
    if let refKind {
      bindText(stmt, bindIdx, refKind); bindIdx += 1
    }
    if let language {
      bindText(stmt, bindIdx, language); bindIdx += 1
    }
    sqlite3_bind_int(stmt, bindIdx, Int32(limit))

    var results: [RAGSymbolReferenceResult] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let repoIdentifier = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let repoName = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
      let rootPath = sqlite3_column_text(stmt, 2).map { String(cString: $0) } ?? ""
      let relPath = sqlite3_column_text(stmt, 3).map { String(cString: $0) } ?? ""
      let language = sqlite3_column_text(stmt, 4).map { String(cString: $0) }
      let kind = sqlite3_column_text(stmt, 5).map { String(cString: $0) } ?? "type"

      results.append(RAGSymbolReferenceResult(
        repoIdentifier: repoIdentifier,
        repoName: repoName,
        repoRootPath: rootPath,
        relativePath: relPath,
        language: language,
        refKind: kind
      ))
    }
    return results
  }

  /// Total reference count for `name` without materializing rows.
  ///
  /// Useful when callers want to gauge popularity before paging — e.g.
  /// "Component" might have 152 hits; a caller can show that first, then
  /// let the user narrow the search by passing a `repoPath`.
  ///
  /// `refKind` accepts the same values as `findSymbolReferences`. When
  /// supplied, the count reflects only that subset — useful when a
  /// caller wants "how many implementers does protocol X have?"
  /// without paging the full result set.
  public func countSymbolReferences(
    name: String,
    repoPath: String? = nil,
    refKind: String? = nil,
    language: String? = nil
  ) throws -> Int {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    let scopedRepoId: String?
    if let repoPath {
      guard let resolved = try resolveRepo(for: repoPath) else { return 0 }
      scopedRepoId = resolved.id
    } else {
      scopedRepoId = nil
    }

    // Language filter forces a join to files since symbol_refs has no
    // language column of its own. Without it we keep the simple form
    // for the unfiltered fast path.
    var sql: String
    if language == nil {
      sql = "SELECT COUNT(*) FROM symbol_refs WHERE referenced_name = ?"
      if scopedRepoId != nil { sql += " AND repo_id = ?" }
      if refKind != nil { sql += " AND ref_kind = ?" }
    } else {
      sql = """
        SELECT COUNT(*)
        FROM symbol_refs sr
        JOIN files f ON f.id = sr.source_file_id
        WHERE sr.referenced_name = ?
        """
      if scopedRepoId != nil { sql += " AND sr.repo_id = ?" }
      if refKind != nil { sql += " AND sr.ref_kind = ?" }
      sql += " AND LOWER(f.language) = LOWER(?)"
    }

    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return 0 }
    defer { sqlite3_finalize(stmt) }

    var bindIdx: Int32 = 1
    bindText(stmt, bindIdx, name); bindIdx += 1
    if let scopedRepoId {
      bindText(stmt, bindIdx, scopedRepoId); bindIdx += 1
    }
    if let refKind {
      bindText(stmt, bindIdx, refKind); bindIdx += 1
    }
    if let language {
      bindText(stmt, bindIdx, language)
    }

    if sqlite3_step(stmt) == SQLITE_ROW {
      return Int(sqlite3_column_int(stmt, 0))
    }
    return 0
  }
}
