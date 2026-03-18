//
//  RAGStore+Audit.swift
//  RAGCore
//
//  Anti-pattern detection via text pattern matching on indexed code chunks.
//

import CSQLite
import Foundation

// MARK: - Audit Types

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

  public init(patternName: String, severity: String, description: String, filePath: String, startLine: Int, endLine: Int, snippet: String, constructName: String?, constructType: String?, matchCount: Int) {
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

// MARK: - Built-in Patterns

extension RAGStore {
  public static let builtInAuditPatterns: [AuditPatternDefinition] = [
    AuditPatternDefinition(
      name: "silent-try",
      pattern: "try\\?\\s",
      severity: "warning",
      description: "Silent try? swallows errors without logging — can hide bugs"
    ),
    AuditPatternDefinition(
      name: "force-unwrap",
      pattern: "\\w+!\\.",
      severity: "info",
      description: "Force unwrap may crash at runtime"
    ),
    AuditPatternDefinition(
      name: "main-sync-dispatch",
      pattern: "DispatchQueue\\.main\\.sync",
      severity: "error",
      description: "Synchronous dispatch to main thread can deadlock"
    ),
    AuditPatternDefinition(
      name: "thread-sleep",
      pattern: "Thread\\.sleep",
      severity: "error",
      description: "Blocking sleep call — use Task.sleep for async code"
    ),
    AuditPatternDefinition(
      name: "print-statement",
      pattern: "\\bprint\\(",
      severity: "info",
      description: "Print statement — consider using os.log or Logger instead"
    ),
    AuditPatternDefinition(
      name: "todo-fixme",
      pattern: "//\\s*(TODO|FIXME|HACK|XXX)",
      severity: "info",
      description: "Unresolved TODO/FIXME comment"
    ),
    AuditPatternDefinition(
      name: "large-tuple",
      pattern: "->\\s*\\([^)]{80,}\\)",
      severity: "warning",
      description: "Large return tuple — consider using a struct instead"
    ),
    AuditPatternDefinition(
      name: "nested-closure",
      pattern: "\\{[^}]*\\{[^}]*\\{[^}]*\\{",
      severity: "warning",
      description: "Deeply nested closures reduce readability"
    ),
  ]
}

// MARK: - Audit Methods

extension RAGStore {
  public func auditAntiPatterns(
    repoPath: String? = nil,
    patterns: [AuditPatternDefinition]? = nil,
    language: String? = nil,
    limit: Int = 50
  ) throws -> [String: [AuditMatch]] {
    try openIfNeeded()

    let activePatterns = patterns ?? Self.builtInAuditPatterns
    guard !activePatterns.isEmpty else { return [:] }

    var compiledPatterns: [(def: AuditPatternDefinition, regex: NSRegularExpression)] = []
    for def in activePatterns {
      if let regex = try? NSRegularExpression(pattern: def.pattern, options: [.anchorsMatchLines]) {
        compiledPatterns.append((def, regex))
      }
    }
    guard !compiledPatterns.isEmpty else { return [:] }

    let resolvedRepoId: String?
    var sql: String

    if let repoPath {
      resolvedRepoId = try resolveRepoId(for: repoPath)
      sql = """
        SELECT c.text, c.start_line, c.end_line, c.construct_name, c.construct_type,
               f.path, r.root_path
        FROM chunks c
        JOIN files f ON c.file_id = f.id
        JOIN repos r ON f.repo_id = r.id
        WHERE r.id = ?
        """
    } else {
      resolvedRepoId = nil
      sql = """
        SELECT c.text, c.start_line, c.end_line, c.construct_name, c.construct_type,
               f.path, r.root_path
        FROM chunks c
        JOIN files f ON c.file_id = f.id
        JOIN repos r ON f.repo_id = r.id
        WHERE 1=1
        """
    }

    if let language {
      sql += " AND f.language = '\(language)'"
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

      let range = NSRange(text.startIndex..., in: text)

      for (def, regex) in compiledPatterns {
        let matches = regex.numberOfMatches(in: text, range: range)
        if matches > 0 {
          let snippet = String(text.prefix(300))
          let match = AuditMatch(
            patternName: def.name,
            severity: def.severity,
            description: def.description,
            filePath: fullPath,
            startLine: startLine,
            endLine: endLine,
            snippet: snippet,
            constructName: constructName,
            constructType: constructType,
            matchCount: matches
          )
          grouped[def.name, default: []].append(match)
          totalMatches += 1
          if totalMatches >= limit { break }
        }
      }
    }

    return grouped
  }
}
//  RAGStore+Audit.swift
//  RAGCore
//
//  Anti-pattern detection via text pattern matching on indexed code chunks.
//

import CSQLite
import Foundation

// MARK: - Audit Types

public struct AuditPatternDefinition: Sendable {
  public let name: String
  public let pattern: String        // Regex pattern
  public let severity: String       // "error", "warning", "info"
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

  public init(patternName: String, severity: String, description: String, filePath: String, startLine: Int, endLine: Int, snippet: String, constructName: String?, constructType: String?, matchCount: Int) {
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

// MARK: - Built-in Patterns

extension RAGStore {
  public static let builtInAuditPatterns: [AuditPatternDefinition] = [
    AuditPatternDefinition(
      name: "silent-try",
      pattern: "try\\?\\s",
      severity: "warning",
      description: "Silent try? swallows errors without logging — can hide bugs"
    ),
    AuditPatternDefinition(
      name: "force-unwrap",
      pattern: "\\w+!\\.",
      severity: "info",
      description: "Force unwrap may crash at runtime"
    ),
    AuditPatternDefinition(
      name: "main-sync-dispatch",
      pattern: "DispatchQueue\\.main\\.sync",
      severity: "error",
      description: "Synchronous dispatch to main thread can deadlock"
    ),
    AuditPatternDefinition(
      name: "thread-sleep",
      pattern: "Thread\\.sleep",
      severity: "error",
      description: "Blocking sleep call — use Task.sleep for async code"
    ),
    AuditPatternDefinition(
      name: "print-statement",
      pattern: "\\bprint\\(",
      severity: "info",
      description: "Print statement — consider using os.log or Logger instead"
    ),
    AuditPatternDefinition(
      name: "todo-fixme",
      pattern: "//\\s*(TODO|FIXME|HACK|XXX)",
      severity: "info",
      description: "Unresolved TODO/FIXME comment"
    ),
    AuditPatternDefinition(
      name: "large-tuple",
      pattern: "->\\s*\\([^)]{80,}\\)",
      severity: "warning",
      description: "Large return tuple — consider using a struct instead"
    ),
    AuditPatternDefinition(
      name: "nested-closure",
      pattern: "\\{[^}]*\\{[^}]*\\{[^}]*\\{",
      severity: "warning",
      description: "Deeply nested closures reduce readability"
    ),
  ]
}

// MARK: - Audit Methods

extension RAGStore {
  /// Audit indexed code for anti-patterns using regex matching.
  /// Returns matches grouped by pattern name.
  public func auditAntiPatterns(
    repoPath: String? = nil,
    patterns: [AuditPatternDefinition]? = nil,
    language: String? = nil,
    limit: Int = 50
  ) throws -> [String: [AuditMatch]] {
    try openIfNeeded()

    let activePatterns = patterns ?? Self.builtInAuditPatterns
    guard !activePatterns.isEmpty else { return [:] }

    // Compile regex patterns upfront
    var compiledPatterns: [(def: AuditPatternDefinition, regex: NSRegularExpression)] = []
    for def in activePatterns {
      if let regex = try? NSRegularExpression(pattern: def.pattern, options: [.anchorsMatchLines]) {
        compiledPatterns.append((def, regex))
      }
    }
    guard !compiledPatterns.isEmpty else { return [:] }

    // Fetch chunks
    let resolvedRepoId: String?
    var sql: String

    if let repoPath {
      resolvedRepoId = try resolveRepoId(for: repoPath)
      sql = """
        SELECT c.text, c.start_line, c.end_line, c.construct_name, c.construct_type,
               f.path, r.root_path
        FROM chunks c
        JOIN files f ON c.file_id = f.id
        JOIN repos r ON f.repo_id = r.id
        WHERE r.id = ?
        """
    } else {
      resolvedRepoId = nil
      sql = """
        SELECT c.text, c.start_line, c.end_line, c.construct_name, c.construct_type,
               f.path, r.root_path
        FROM chunks c
        JOIN files f ON c.file_id = f.id
        JOIN repos r ON f.repo_id = r.id
        WHERE 1=1
        """
    }

    if let language {
      sql += " AND f.language = '\(language)'"
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

      let range = NSRange(text.startIndex..., in: text)

      for (def, regex) in compiledPatterns {
        let matches = regex.numberOfMatches(in: text, range: range)
        if matches > 0 {
          let snippet = String(text.prefix(300))
          let match = AuditMatch(
            patternName: def.name,
            severity: def.severity,
            description: def.description,
            filePath: fullPath,
            startLine: startLine,
            endLine: endLine,
            snippet: snippet,
            constructName: constructName,
            constructType: constructType,
            matchCount: matches
          )
          grouped[def.name, default: []].append(match)
          totalMatches += 1
          if totalMatches >= limit { break }
        }
      }
    }

    return grouped
  }
}
