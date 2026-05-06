//
//  RAGStore+Coverage.swift
//  RAGCore
//
//  Index-coverage diagnostics: surface the gap between what's on disk
//  and what's actually in the index. Refactor and search workflows
//  silently miss files that fell out of the index (or never made it in)
//  — without this, agents can't tell the difference between "X doesn't
//  exist" and "X exists but isn't searchable."
//

import CSQLite
import Foundation

// MARK: - Result Types

/// One file present on disk but missing from the chunks/files index.
public struct RAGUnindexedFile: Sendable {
  /// Repo-relative path (e.g. "macOS/Applications/SwarmDashboardView.swift").
  public let relativePath: String
  /// Source language as detected by the scanner (e.g. "Swift").
  public let language: String
  /// File size in bytes, capped at the scanner's `maxFileBytes`.
  public let byteCount: Int

  public init(relativePath: String, language: String, byteCount: Int) {
    self.relativePath = relativePath
    self.language = language
    self.byteCount = byteCount
  }
}

/// Summary of index coverage for a repo. Counts on-disk vs. indexed,
/// broken down per language so refactor work can spot patterns ("Swift
/// is fully covered but Ruby is at 60%").
public struct RAGCoverageReport: Sendable {
  public struct LanguageCoverage: Sendable {
    public let language: String
    public let onDisk: Int
    public let indexed: Int
    /// Disk count minus indexed count, floored at 0.
    public var gap: Int { max(0, onDisk - indexed) }

    public init(language: String, onDisk: Int, indexed: Int) {
      self.language = language
      self.onDisk = onDisk
      self.indexed = indexed
    }
  }

  public let repoIdentifier: String
  public let repoName: String
  public let totalOnDisk: Int
  public let totalIndexed: Int
  public let byLanguage: [LanguageCoverage]
  /// Sample of paths on disk but missing from the index, capped at the
  /// caller's `limit`. Excluded files (per .ragignore / scanner rules)
  /// are not counted as gaps — only indexable files that should be
  /// there.
  public let missingSample: [RAGUnindexedFile]

  public init(
    repoIdentifier: String,
    repoName: String,
    totalOnDisk: Int,
    totalIndexed: Int,
    byLanguage: [LanguageCoverage],
    missingSample: [RAGUnindexedFile]
  ) {
    self.repoIdentifier = repoIdentifier
    self.repoName = repoName
    self.totalOnDisk = totalOnDisk
    self.totalIndexed = totalIndexed
    self.byLanguage = byLanguage
    self.missingSample = missingSample
  }
}

extension RAGStore {

  /// Compare what the scanner sees on disk against what's in the
  /// chunks/files index for `repoPath`, and return a coverage report
  /// plus a sample of missing paths.
  ///
  /// Useful when an agent or operator suspects "rag.search returned
  /// nothing, but I know the file exists" — this names the gap directly
  /// instead of forcing an investigation. The scanner's exclusion rules
  /// (.ragignore, default-excluded directories, file-size limits, etc.)
  /// are honored so this only flags files the scanner *would* have
  /// indexed.
  ///
  /// - Parameters:
  ///   - repoPath: Repo path (or any value `resolveRepo` accepts).
  ///   - missingSampleLimit: Cap on how many missing-file rows to
  ///     return in `missingSample`. Aggregate counts (`totalOnDisk`,
  ///     `byLanguage`) are exact regardless. Defaults to 50.
  /// - Returns: Coverage report. `nil` if the repo isn't registered.
  public func coverage(
    for repoPath: String,
    missingSampleLimit: Int = 50
  ) throws -> RAGCoverageReport? {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    guard let resolved = try resolveRepo(for: repoPath) else { return nil }
    let repoId = resolved.id

    // Look up display name + identifier (ResolvedRepo only carries id +
    // rootPath, but the report fields are operator-facing).
    var repoName = ""
    var repoIdentifier = ""
    do {
      var nameStmt: OpaquePointer?
      if sqlite3_prepare_v2(db, "SELECT name, repo_identifier FROM repos WHERE id = ?", -1, &nameStmt, nil) == SQLITE_OK, let nameStmt {
        defer { sqlite3_finalize(nameStmt) }
        bindText(nameStmt, 1, repoId)
        if sqlite3_step(nameStmt) == SQLITE_ROW {
          repoName = sqlite3_column_text(nameStmt, 0).map { String(cString: $0) } ?? ""
          repoIdentifier = sqlite3_column_text(nameStmt, 1).map { String(cString: $0) } ?? ""
        }
      }
    }

    // 1. Walk the disk via the same scanner the indexer uses so we
    //    honor .ragignore + default exclusions exactly.
    let rootURL = URL(fileURLWithPath: resolved.rootPath)
    let candidates = scanner.scan(rootURL: rootURL)

    // 2. Build language-keyed buckets of on-disk relative paths.
    let rootPrefix = resolved.rootPath.hasSuffix("/") ? resolved.rootPath : resolved.rootPath + "/"
    var diskByLanguage: [String: [(rel: String, lang: String, bytes: Int)]] = [:]
    var diskAll: [String: (lang: String, bytes: Int)] = [:]
    for c in candidates {
      let rel = c.path.hasPrefix(rootPrefix) ? String(c.path.dropFirst(rootPrefix.count)) : c.path
      diskAll[rel] = (c.language, c.byteCount)
      diskByLanguage[c.language, default: []].append((rel, c.language, c.byteCount))
    }

    // 3. Pull the indexed paths for this repo. We only care about path
    //    + language, so the SELECT stays narrow.
    var indexedPaths = Set<String>()
    var indexedByLanguage: [String: Int] = [:]
    let sql = "SELECT path, language FROM files WHERE repo_id = ?"
    var stmt: OpaquePointer?
    if sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt {
      defer { sqlite3_finalize(stmt) }
      bindText(stmt, 1, repoId)
      while sqlite3_step(stmt) == SQLITE_ROW {
        let path = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
        let lang = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? "unknown"
        indexedPaths.insert(path)
        indexedByLanguage[lang, default: 0] += 1
      }
    }

    // 4. Build per-language summaries and the missing-sample list.
    let allLanguages = Set(diskByLanguage.keys).union(indexedByLanguage.keys)
    let byLanguage = allLanguages
      .map { lang in
        RAGCoverageReport.LanguageCoverage(
          language: lang,
          onDisk: diskByLanguage[lang]?.count ?? 0,
          indexed: indexedByLanguage[lang] ?? 0
        )
      }
      .sorted { $0.onDisk > $1.onDisk }

    var missing: [RAGUnindexedFile] = []
    for (rel, info) in diskAll where !indexedPaths.contains(rel) {
      missing.append(RAGUnindexedFile(
        relativePath: rel,
        language: info.lang,
        byteCount: info.bytes
      ))
      if missing.count >= missingSampleLimit { break }
    }
    // Sort by language then path for stable output.
    missing.sort {
      if $0.language != $1.language { return $0.language < $1.language }
      return $0.relativePath < $1.relativePath
    }

    return RAGCoverageReport(
      repoIdentifier: repoIdentifier,
      repoName: repoName,
      totalOnDisk: candidates.count,
      totalIndexed: indexedPaths.count,
      byLanguage: byLanguage,
      missingSample: missing
    )
  }
}
