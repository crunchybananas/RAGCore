//
//  RAGStore+Audit.swift
//  RAGCore
//
//  Anti-pattern detection via text pattern matching on indexed code chunks.
//

import CSQLite
import Foundation

public struct AuditPatternDefinition: Sendable {
  public let name: String
  public let pattern: String
  public let severity: String
  public let description: String

  public init(name: String, pattern: String, severity: String, description: String) {
    self.name = name
    self.pattern = pattern
    self.severity = severity
    self.description = description
  }
}

public struct AuditMatch: Sendable {
  public let patternName: String
  public let severity: String
  public let description: String
  public let filePath: String
  public let startLine: Int
  public let endLine: Int
  public let snippet: String
  public let constructName: String?
  public let constructType: String?
  public let matchCount: Int

  public init(
    patternName: String,
    severity: String,
    description: String,
    filePath: String,
    startLine: Int,
    endLine: Int,
    snippet: String,
    constructName: String?,
    constructType: String?,
    matchCount: Int
  ) {
    self.patternName = patternName
    self.severity = severity
    self.description = description
    self.filePath = filePath
    self.startLine = startLine
    self.endLine = endLine
    self.snippet = snippet
    self.constructName = constructName
    self.constructType = constructType
    self.matchCount = matchCount
  }
}

extension RAGStore {
  public static let builtInAuditPatterns: [AuditPatternDefinition] = [
    AuditPatternDefinition(
      name: "silent-try",
      pattern: "try\\?\\s",
      severity: "warning",
      description: "Silent try? swallows errors without logging and can hide bugs."
    ),
    AuditPatternDefinition(
      name: "force-unwrap",
      pattern: "\\w+!\\.",
      severity: "info",
      description: "Force unwrap may crash at runtime."
    ),
    AuditPatternDefinition(
      name: "main-sync-dispatch",
      pattern: "DispatchQueue\\.main\\.sync",
      severity: "error",
      description: "Synchronous dispatch to the main queue can deadlock."
    ),
    AuditPatternDefinition(
      name: "thread-sleep",
      pattern: "Thread\\.sleep",
      severity: "error",
      description: "Blocking sleep call; prefer Task.sleep in async code."
    ),
    AuditPatternDefinition(
      name: "print-statement",
      pattern: "\\bprint\\(",
      severity: "info",
      description: "Print statement found; consider Logger or os_log instead."
    ),
    AuditPatternDefinition(
      name: "todo-fixme",
      pattern: "//\\s*(TODO|FIXME|HACK|XXX)",
      severity: "info",
      description: "Unresolved TODO/FIXME style comment."
    ),
    AuditPatternDefinition(
      name: "large-tuple",
      pattern: "->\\s*\\([^)]{80,}\\)",
      severity: "warning",
      description: "Large tuple return type; consider a named struct instead."
    ),
    AuditPatternDefinition(
      name: "nested-closure",
      pattern: "\\{[^}]*\\{[^}]*\\{[^}]*\\{",
      severity: "warning",
      description: "Deeply nested closures reduce readability."
    ),
  ]

  public func auditAntiPatterns(
    repoPath: String? = nil,
    patterns: [AuditPatternDefinition]? = nil,
    language: String? = nil,
    limit: Int = 50
  ) throws -> [String: [AuditMatch]] {
    try openIfNeeded()

    let activePatterns = patterns ?? Self.builtInAuditPatterns
    guard !activePatterns.isEmpty else { return [:] }

    var compiledPatterns: [(definition: AuditPatternDefinition, regex: NSRegularExpression)] = []
    for definition in activePatterns {
      if let regex = try? NSRegularExpression(pattern: definition.pattern, options: [.anchorsMatchLines]) {
        compiledPatterns.append((definition, regex))
      }
    }
    guard !compiledPatterns.isEmpty else { return [:] }

    let resolvedRepoId: String?
    var sql = """
      SELECT c.text, c.start_line, c.end_line, c.construct_name, c.construct_type,
             f.path, r.root_path
      FROM chunks c
      JOIN files f ON c.file_id = f.id
      JOIN repos r ON f.repo_id = r.id
      WHERE 1 = 1
      """

    if let repoPath {
      resolvedRepoId = try resolveRepoId(for: repoPath)
      sql += " AND r.id = ?"
    } else {
      resolvedRepoId = nil
    }

    if let language {
      sql += " AND f.language = '\(language)'"
    }

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var statement: OpaquePointer?
    let prepareResult = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard prepareResult == SQLITE_OK, let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }

    if let resolvedRepoId {
      bindText(statement, 1, resolvedRepoId)
    }

    var grouped: [String: [AuditMatch]] = [:]
    var totalMatches = 0

    while sqlite3_step(statement) == SQLITE_ROW {
      guard totalMatches < limit else { break }

      let text = String(cString: sqlite3_column_text(statement, 0))
      let startLine = Int(sqlite3_column_int(statement, 1))
      let endLine = Int(sqlite3_column_int(statement, 2))
      let constructName = sqlite3_column_text(statement, 3).map { String(cString: $0) }
      let constructType = sqlite3_column_text(statement, 4).map { String(cString: $0) }
      let filePath = String(cString: sqlite3_column_text(statement, 5))
      let rootPath = String(cString: sqlite3_column_text(statement, 6))
      let fullPath = rootPath + "/" + filePath
      let searchRange = NSRange(text.startIndex..., in: text)

      for (definition, regex) in compiledPatterns {
        let matches = regex.numberOfMatches(in: text, range: searchRange)
        guard matches > 0 else { continue }

        let match = AuditMatch(
          patternName: definition.name,
          severity: definition.severity,
          description: definition.description,
          filePath: fullPath,
          startLine: startLine,
          endLine: endLine,
          snippet: String(text.prefix(300)),
          constructName: constructName,
          constructType: constructType,
          matchCount: matches
        )
        grouped[definition.name, default: []].append(match)
        totalMatches += 1

        if totalMatches >= limit {
          break
        }
      }
    }

    return grouped
  }
}
