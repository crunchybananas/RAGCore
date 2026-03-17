//
//  RAGStore+TagQuery.swift
//  RAGCore
//
//  Query chunks by AI-generated tags and list available tags with counts.
//

import CSQLite
import Foundation

// MARK: - Tag Query Types

public struct TagQueryResult: Sendable {
  public let filePath: String
  public let startLine: Int
  public let endLine: Int
  public let constructName: String?
  public let constructType: String?
  public let snippet: String
  public let aiSummary: String?
  public let aiTags: [String]
  public let tokenCount: Int

  public init(filePath: String, startLine: Int, endLine: Int, constructName: String?, constructType: String?, snippet: String, aiSummary: String?, aiTags: [String], tokenCount: Int) {
    self.filePath = filePath
    self.startLine = startLine
    self.endLine = endLine
    self.constructName = constructName
    self.constructType = constructType
    self.snippet = snippet
    self.aiSummary = aiSummary
    self.aiTags = aiTags
    self.tokenCount = tokenCount
  }
}

// MARK: - Tag Query Methods

extension RAGStore {

  /// Query chunks by AI-generated tags.
  /// When matchAll is true, chunks must have ALL specified tags. Otherwise any tag matches.
  public func queryByTags(
    tags: [String],
    repoPath: String? = nil,
    constructType: String? = nil,
    matchAll: Bool = false,
    limit: Int = 30
  ) throws -> [TagQueryResult] {
    try openIfNeeded()
    guard !tags.isEmpty else { return [] }

    let resolvedRepoId: String?
    var sql = """
      SELECT c.text, c.start_line, c.end_line, c.construct_name, c.construct_type,
             c.ai_summary, c.ai_tags, c.token_count, f.path, r.root_path
      FROM chunks c
      JOIN files f ON c.file_id = f.id
      JOIN repos r ON f.repo_id = r.id
      WHERE c.ai_tags IS NOT NULL
      """

    if let repoPath {
      resolvedRepoId = try resolveRepoId(for: repoPath)
      sql += " AND r.id = ?"
    } else {
      resolvedRepoId = nil
    }

    if let constructType {
      sql += " AND c.construct_type = '\(constructType)'"
    }

    // Build tag filter: each tag matched with LIKE on the JSON array string
    let connector = matchAll ? " AND " : " OR "
    let tagClauses = tags.map { _ in "c.ai_tags LIKE ?" }
    sql += " AND (" + tagClauses.joined(separator: connector) + ")"

    sql += " ORDER BY c.token_count DESC LIMIT ?"

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }

    var bindIndex: Int32 = 1
    if let resolvedRepoId {
      bindText(statement, bindIndex, resolvedRepoId)
      bindIndex += 1
    }
    for tag in tags {
      let likePattern = "%\"\(tag)\"%"
      bindText(statement, bindIndex, likePattern)
      bindIndex += 1
    }
    sqlite3_bind_int(statement, bindIndex, Int32(limit))

    var results: [TagQueryResult] = []
    while sqlite3_step(statement) == SQLITE_ROW {
      let text = String(cString: sqlite3_column_text(statement, 0))
      let startLine = Int(sqlite3_column_int(statement, 1))
      let endLine = Int(sqlite3_column_int(statement, 2))
      let constructName = sqlite3_column_text(statement, 3).map { String(cString: $0) }
      let cType = sqlite3_column_text(statement, 4).map { String(cString: $0) }
      let aiSummary = sqlite3_column_text(statement, 5).map { String(cString: $0) }
      let aiTagsRaw = sqlite3_column_text(statement, 6).map { String(cString: $0) }
      let tokenCount = Int(sqlite3_column_int(statement, 7))
      let filePath = String(cString: sqlite3_column_text(statement, 8))
      let rootPath = String(cString: sqlite3_column_text(statement, 9))

      let parsedTags: [String]
      if let raw = aiTagsRaw, let data = raw.data(using: .utf8),
         let decoded = try? JSONDecoder().decode([String].self, from: data) {
        parsedTags = decoded
      } else {
        parsedTags = []
      }

      results.append(TagQueryResult(
        filePath: rootPath + "/" + filePath,
        startLine: startLine,
        endLine: endLine,
        constructName: constructName,
        constructType: cType,
        snippet: String(text.prefix(300)),
        aiSummary: aiSummary,
        aiTags: parsedTags,
        tokenCount: tokenCount
      ))
    }

    return results
  }

  /// Get all distinct AI tags with their occurrence counts.
  public func getTagCounts(repoPath: String? = nil) throws -> [(tag: String, count: Int)] {
    try openIfNeeded()

    let resolvedRepoId: String?
    var sql: String

    if let repoPath {
      resolvedRepoId = try resolveRepoId(for: repoPath)
      sql = """
        SELECT c.ai_tags FROM chunks c
        JOIN files f ON c.file_id = f.id
        JOIN repos r ON f.repo_id = r.id
        WHERE c.ai_tags IS NOT NULL AND r.id = ?
        """
    } else {
      resolvedRepoId = nil
      sql = """
        SELECT c.ai_tags FROM chunks c
        JOIN files f ON c.file_id = f.id
        WHERE c.ai_tags IS NOT NULL
        """
    }

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }

    if let resolvedRepoId {
      bindText(statement, 1, resolvedRepoId)
    }

    var tagCounts: [String: Int] = [:]
    let decoder = JSONDecoder()

    while sqlite3_step(statement) == SQLITE_ROW {
      let raw = String(cString: sqlite3_column_text(statement, 0))
      guard let data = raw.data(using: .utf8),
            let tags = try? decoder.decode([String].self, from: data) else { continue }
      for tag in tags {
        tagCounts[tag, default: 0] += 1
      }
    }

    return tagCounts.sorted { $0.value > $1.value }.map { (tag: $0.key, count: $0.value) }
  }
}
