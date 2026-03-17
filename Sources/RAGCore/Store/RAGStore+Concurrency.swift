//
//  RAGStore+Concurrency.swift
//  RAGCore
//
//  Swift concurrency audit: detects @MainActor misuse, isolation anti-patterns,
//  and broadcast amplification in indexed code.
//

import CSQLite
import Foundation

// MARK: - Concurrency Audit Types

public struct ConcurrencyFinding: Sendable {
  public let category: String       // "mainactor-heavyweight", "task-inherits-isolation", "nonisolated-candidate", "broadcast-amplification"
  public let severity: String       // "error", "warning", "info"
  public let filePath: String
  public let startLine: Int
  public let endLine: Int
  public let constructName: String?
  public let constructType: String?
  public let detail: String
  public let snippet: String

  public init(category: String, severity: String, filePath: String, startLine: Int, endLine: Int, constructName: String?, constructType: String?, detail: String, snippet: String) {
    self.category = category
    self.severity = severity
    self.filePath = filePath
    self.startLine = startLine
    self.endLine = endLine
    self.constructName = constructName
    self.constructType = constructType
    self.detail = detail
    self.snippet = snippet
  }
}

// MARK: - Concurrency Audit

extension RAGStore {

  /// Audit Swift concurrency patterns across indexed code.
  /// Detects: @MainActor classes with heavy I/O, Task{} inheriting isolation,
  /// nonisolated-worthy methods, and broadcast amplification loops.
  public func auditConcurrency(
    repoPath: String? = nil,
    categories: [String]? = nil,
    limit: Int = 50
  ) throws -> [ConcurrencyFinding] {
    try openIfNeeded()

    let allCategories = Set(categories ?? ["mainactor-heavyweight", "task-inherits-isolation", "nonisolated-candidate", "broadcast-amplification"])

    // Fetch Swift chunks only
    let resolvedRepoId: String?
    let sql: String

    if let repoPath {
      resolvedRepoId = try resolveRepoId(for: repoPath)
      sql = """
        SELECT c.text, c.start_line, c.end_line, c.construct_name, c.construct_type,
               f.path, r.root_path
        FROM chunks c
        JOIN files f ON c.file_id = f.id
        JOIN repos r ON f.repo_id = r.id
        WHERE r.id = ? AND (f.language = 'swift' OR f.language = 'Swift')
        """
    } else {
      resolvedRepoId = nil
      sql = """
        SELECT c.text, c.start_line, c.end_line, c.construct_name, c.construct_type,
               f.path, r.root_path
        FROM chunks c
        JOIN files f ON c.file_id = f.id
        JOIN repos r ON f.repo_id = r.id
        WHERE (f.language = 'swift' OR f.language = 'Swift')
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

    // Compile detection patterns
    let mainActorClassRegex = try NSRegularExpression(pattern: "@MainActor\\s+(?:@\\w+\\s+)*(?:public\\s+|private\\s+|internal\\s+|fileprivate\\s+|open\\s+)*(?:final\\s+)?class\\s+(\\w+)", options: [])
    let mainActorRunTaskRegex = try NSRegularExpression(pattern: "MainActor\\.run\\s*\\{[^}]*Task\\s*\\{", options: [.dotMatchesLineSeparators])
    let ioPatternRegex = try NSRegularExpression(pattern: "JSONEncoder\\(\\)|JSONDecoder\\(\\)|JSONSerialization|FileManager\\.default|Data\\(contentsOf|URLSession\\.shared|FileHandle|\\.write\\(to", options: [])
    let broadcastRegex = try NSRegularExpression(pattern: "for\\s+\\w+\\s+in\\s+\\w+[^{]*\\{[^}]*(send|broadcast|notify|post|emit|channel\\.send|try\\?.*await.*send)\\(", options: [.dotMatchesLineSeparators])

    var findings: [ConcurrencyFinding] = []

    while sqlite3_step(statement) == SQLITE_ROW {
      guard findings.count < limit else { break }

      let text = String(cString: sqlite3_column_text(statement, 0))
      let startLine = Int(sqlite3_column_int(statement, 1))
      let endLine = Int(sqlite3_column_int(statement, 2))
      let constructName = sqlite3_column_text(statement, 3).map { String(cString: $0) }
      let constructType = sqlite3_column_text(statement, 4).map { String(cString: $0) }
      let filePath = String(cString: sqlite3_column_text(statement, 5))
      let rootPath = String(cString: sqlite3_column_text(statement, 6))
      let fullPath = rootPath + "/" + filePath
      let range = NSRange(text.startIndex..., in: text)
      let snippet = String(text.prefix(400))

      // 1. @MainActor class with I/O operations
      if allCategories.contains("mainactor-heavyweight") {
        let mainActorMatches = mainActorClassRegex.numberOfMatches(in: text, range: range)
        if mainActorMatches > 0 {
          let ioMatches = ioPatternRegex.numberOfMatches(in: text, range: range)
          if ioMatches > 0 {
            findings.append(ConcurrencyFinding(
              category: "mainactor-heavyweight",
              severity: "warning",
              filePath: fullPath,
              startLine: startLine,
              endLine: endLine,
              constructName: constructName,
              constructType: constructType,
              detail: "@MainActor class contains \(ioMatches) I/O operations (JSON encoding, file access, network) that should be nonisolated to avoid blocking the main thread",
              snippet: snippet
            ))
          }
        }
      }

      // 2. Task{} inside MainActor.run — inherits isolation
      if allCategories.contains("task-inherits-isolation") {
        let taskInMainActor = mainActorRunTaskRegex.numberOfMatches(in: text, range: range)
        if taskInMainActor > 0 {
          findings.append(ConcurrencyFinding(
            category: "task-inherits-isolation",
            severity: "error",
            filePath: fullPath,
            startLine: startLine,
            endLine: endLine,
            constructName: constructName,
            constructType: constructType,
            detail: "Task{} inside MainActor.run inherits @MainActor isolation — use Task.detached or move work outside MainActor.run",
            snippet: snippet
          ))
        }
      }

      // 3. Functions in @MainActor context doing heavy I/O (nonisolated candidates)
      if allCategories.contains("nonisolated-candidate") {
        // Look for func declarations with I/O but no nonisolated annotation
        let hasMainActor = text.contains("@MainActor") || text.contains("MainActor")
        let hasIO = ioPatternRegex.numberOfMatches(in: text, range: range) > 0
        let isNonisolated = text.contains("nonisolated")
        let isFunc = constructType == "function" || constructType == "method"

        if isFunc && hasMainActor && hasIO && !isNonisolated {
          findings.append(ConcurrencyFinding(
            category: "nonisolated-candidate",
            severity: "warning",
            filePath: fullPath,
            startLine: startLine,
            endLine: endLine,
            constructName: constructName,
            constructType: constructType,
            detail: "Function in @MainActor context performs I/O (JSON, file, network) — consider marking nonisolated",
            snippet: snippet
          ))
        }
      }

      // 4. Broadcast amplification — iterating peers/connections and sending
      if allCategories.contains("broadcast-amplification") {
        let broadcastMatches = broadcastRegex.numberOfMatches(in: text, range: range)
        if broadcastMatches > 0 {
          findings.append(ConcurrencyFinding(
            category: "broadcast-amplification",
            severity: "warning",
            filePath: fullPath,
            startLine: startLine,
            endLine: endLine,
            constructName: constructName,
            constructType: constructType,
            detail: "Loop broadcasting to multiple recipients — consider rate limiting or batching to prevent message storms",
            snippet: snippet
          ))
        }
      }
    }

    return findings
  }
}
