//
//  RAGStore+SymbolDefinitions.swift
//  RAGCore
//
//  Symbol definition graph: extracts each chunk's `constructName` /
//  `constructType` and persists them to the `symbols` table so callers
//  can answer "where is X defined?". Companion to RAGStore+Symbols
//  (which exposes the read API) and RAGStore+Dependencies (which
//  handles symbol *references*, not definitions).
//
//  Why this exists: the schema has had a `symbols` table since v1, with
//  three indexes (name, file, repo), but no insert path. The chunker
//  emits constructName for every AST chunk it produces; this hooks that
//  data into the table so refactor workflows can query both halves of
//  the symbol graph (callers and definitions) symmetrically.
//

import ASTChunker
import CSQLite
import Foundation

extension RAGStore {

  // MARK: - Symbol Definition Model

  /// A symbol definition row, mapped 1:1 to the `symbols` SQL table.
  /// Constructed by `extractSymbolDefinitions` from chunks the AST
  /// chunker has already classified (struct/class/protocol/function/etc).
  struct Symbol {
    let id: String
    let repoId: String
    let fileId: String
    let name: String
    /// Definition kind — mirrors the chunker's `constructType` (e.g.
    /// "class", "struct", "function", "method", "protocol", "enum").
    let kind: String
    let startLine: Int?
    let endLine: Int?
  }

  // MARK: - Extraction

  /// Build symbol-definition rows for one file from its AST chunks.
  ///
  /// Skips chunks without a name (e.g. line-based fallback chunks have
  /// `constructName == nil`) and dedupes by `(name, kind)` within the
  /// file so a single class spread across multiple chunks (#extension,
  /// partial class) doesn't generate duplicate rows.
  internal func extractSymbolDefinitions(
    from chunks: [RAGChunk],
    repoId: String,
    fileId: String
  ) -> [Symbol] {
    var symbols: [Symbol] = []
    var seen = Set<String>()

    for chunk in chunks {
      guard let rawName = chunk.constructName?.trimmingCharacters(in: .whitespacesAndNewlines),
            !rawName.isEmpty else { continue }
      // Some chunkers emit qualified names like "Foo.bar"; keep the
      // canonical form the caller would search for.
      let name = rawName

      let kind = (chunk.constructType?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased())
        ?? "unknown"
      // Filter out non-definitional chunk kinds — comments, imports,
      // and "block" chunks are not symbols and would only add noise to
      // the index. Definition-bearing kinds are AST constructs that
      // declare a named entity.
      let definitional: Set<String> = [
        "class", "struct", "enum", "protocol", "interface", "trait",
        "actor", "extension", "function", "method", "init", "deinit",
        "subscript", "module", "namespace", "type", "typealias",
        "constant", "variable", "property", "component", "service",
        "controller", "route", "model", "helper", "modifier", "mixin",
        "macro", "operator",
      ]
      guard definitional.contains(kind) else { continue }

      let dedupKey = "\(name)::\(kind)"
      guard !seen.contains(dedupKey) else { continue }
      seen.insert(dedupKey)

      symbols.append(Symbol(
        id: VectorMath.stableId(for: "\(fileId):def:\(kind):\(name)"),
        repoId: repoId,
        fileId: fileId,
        name: name,
        kind: kind,
        startLine: chunk.startLine,
        endLine: chunk.endLine
      ))
    }

    return symbols
  }

  // MARK: - Insert / Delete

  internal func insertSymbol(_ symbol: Symbol) throws {
    let sql = """
      INSERT OR REPLACE INTO symbols
        (id, repo_id, file_id, name, kind, start_line, end_line)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, symbol.id)
      bindText(stmt, 2, symbol.repoId)
      bindText(stmt, 3, symbol.fileId)
      bindText(stmt, 4, symbol.name)
      bindText(stmt, 5, symbol.kind)
      if let line = symbol.startLine {
        sqlite3_bind_int(stmt, 6, Int32(line))
      } else {
        sqlite3_bind_null(stmt, 6)
      }
      if let line = symbol.endLine {
        sqlite3_bind_int(stmt, 7, Int32(line))
      } else {
        sqlite3_bind_null(stmt, 7)
      }
    }
  }

  internal func insertSymbols(_ symbols: [Symbol]) throws {
    guard !symbols.isEmpty else { return }
    try exec("BEGIN TRANSACTION")
    do {
      for symbol in symbols {
        do { try insertSymbol(symbol) } catch {
          // Same defensive logging style as insertSymbolRef so a single
          // bad row (e.g. FK violation from a race with file deletion)
          // doesn't take down the whole batch.
          print("[RAG] FK error inserting symbol: \(symbol.name) (\(symbol.kind)) — \(error)")
          continue
        }
      }
      try exec("COMMIT")
    } catch {
      try? exec("ROLLBACK")
      throw error
    }
  }

  internal func deleteSymbols(for fileId: String) throws {
    try execute(sql: "DELETE FROM symbols WHERE file_id = ?") { stmt in
      bindText(stmt, 1, fileId)
    }
  }

  internal func deleteSymbols(forRepo repoId: String) throws {
    try execute(sql: "DELETE FROM symbols WHERE repo_id = ?") { stmt in
      bindText(stmt, 1, repoId)
    }
  }

  // MARK: - Public Query

  /// Find every symbol definition matching `name`, optionally scoped to
  /// one repo. Companion to `findSymbolReferences` — together they answer
  /// "where is X defined?" and "who references X?" with the same shape.
  public func findSymbolDefinitions(
    name: String,
    repoPath: String? = nil,
    limit: Int = 100
  ) throws -> [RAGSymbolDefinitionResult] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    let scopedRepoId: String?
    if let repoPath {
      guard let resolved = try resolveRepo(for: repoPath) else { return [] }
      scopedRepoId = resolved.id
    } else {
      scopedRepoId = nil
    }

    var sql = """
      SELECT r.repo_identifier, r.name, r.root_path, f.path, f.language,
             s.kind, s.start_line, s.end_line
      FROM symbols s
      JOIN files f ON f.id = s.file_id
      JOIN repos r ON r.id = s.repo_id
      WHERE s.name = ?
      """
    if scopedRepoId != nil {
      sql += " AND s.repo_id = ?"
    }
    sql += " ORDER BY r.name, f.path, s.start_line LIMIT ?"

    var stmt: OpaquePointer?
    let prepResult = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard prepResult == SQLITE_OK, let stmt else {
      let errMsg = String(cString: sqlite3_errmsg(db))
      throw RAGError.sqlite("findSymbolDefinitions prepare failed: \(errMsg)")
    }
    defer { sqlite3_finalize(stmt) }

    var bindIdx: Int32 = 1
    bindText(stmt, bindIdx, name); bindIdx += 1
    if let scopedRepoId {
      bindText(stmt, bindIdx, scopedRepoId); bindIdx += 1
    }
    sqlite3_bind_int(stmt, bindIdx, Int32(limit))

    var results: [RAGSymbolDefinitionResult] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let repoIdentifier = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let repoName = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
      let rootPath = sqlite3_column_text(stmt, 2).map { String(cString: $0) } ?? ""
      let relPath = sqlite3_column_text(stmt, 3).map { String(cString: $0) } ?? ""
      let language = sqlite3_column_text(stmt, 4).map { String(cString: $0) }
      let kind = sqlite3_column_text(stmt, 5).map { String(cString: $0) } ?? "unknown"
      let startLine = sqlite3_column_type(stmt, 6) == SQLITE_NULL ? nil : Int(sqlite3_column_int(stmt, 6))
      let endLine = sqlite3_column_type(stmt, 7) == SQLITE_NULL ? nil : Int(sqlite3_column_int(stmt, 7))

      results.append(RAGSymbolDefinitionResult(
        repoIdentifier: repoIdentifier,
        repoName: repoName,
        repoRootPath: rootPath,
        relativePath: relPath,
        language: language,
        kind: kind,
        startLine: startLine,
        endLine: endLine
      ))
    }
    return results
  }

  /// Total number of definitions matching `name`. Symmetric with
  /// `countSymbolReferences` so callers can compute a quick definition
  /// + reference count pair before paging the full results.
  public func countSymbolDefinitions(
    name: String,
    repoPath: String? = nil
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

    var sql = "SELECT COUNT(*) FROM symbols WHERE name = ?"
    if scopedRepoId != nil {
      sql += " AND repo_id = ?"
    }

    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return 0 }
    defer { sqlite3_finalize(stmt) }

    var bindIdx: Int32 = 1
    bindText(stmt, bindIdx, name); bindIdx += 1
    if let scopedRepoId {
      bindText(stmt, bindIdx, scopedRepoId)
    }

    if sqlite3_step(stmt) == SQLITE_ROW {
      return Int(sqlite3_column_int(stmt, 0))
    }
    return 0
  }
}

// MARK: - Result Type

/// One match from `findSymbolDefinitions`. Same shape as
/// `RAGSymbolReferenceResult` plus line range so callers can navigate
/// straight to the definition site.
public struct RAGSymbolDefinitionResult: Sendable {
  public let repoIdentifier: String
  public let repoName: String
  public let repoRootPath: String
  public let relativePath: String
  public let language: String?
  /// Definition kind from the chunker: "class", "struct", "function", etc.
  public let kind: String
  public let startLine: Int?
  public let endLine: Int?

  public init(
    repoIdentifier: String,
    repoName: String,
    repoRootPath: String,
    relativePath: String,
    language: String?,
    kind: String,
    startLine: Int?,
    endLine: Int?
  ) {
    self.repoIdentifier = repoIdentifier
    self.repoName = repoName
    self.repoRootPath = repoRootPath
    self.relativePath = relativePath
    self.language = language
    self.kind = kind
    self.startLine = startLine
    self.endLine = endLine
  }
}
