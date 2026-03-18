//
//  RAGStore+Audit.swift
//  RAGCore
//
//  Code audit, concurrency heuristics, and AI tag query helpers.
//

import CSQLite
import Foundation

public struct AuditPatternDefinition: Sendable {
  public let name: String
  public let severity: String
  public let description: String

  public init(name: String, severity: String, description: String) {
    self.name = name
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

public struct ConcurrencyFinding: Sendable {
  public let category: String
  public let severity: String
  public let filePath: String
  public let startLine: Int
  public let endLine: Int
  public let constructName: String?
  public let constructType: String?
  public let detail: String
  public let snippet: String

  public init(
    category: String,
    severity: String,
    filePath: String,
    startLine: Int,
    endLine: Int,
    constructName: String?,
    constructType: String?,
    detail: String,
    snippet: String
  ) {
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

  public init(
    filePath: String,
    startLine: Int,
    endLine: Int,
    constructName: String?,
    constructType: String?,
    snippet: String,
    aiSummary: String?,
    aiTags: [String],
    tokenCount: Int
  ) {
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

extension RAGStore {
  public static let builtInAuditPatterns: [AuditPatternDefinition] = [
    AuditPatternDefinition(name: "silent-try", severity: "warning", description: "Detects `try?` usage that silently discards errors."),
    AuditPatternDefinition(name: "force-unwrap", severity: "error", description: "Detects force-unwrapped optionals or `try!` that can crash at runtime."),
    AuditPatternDefinition(name: "main-sync-dispatch", severity: "error", description: "Detects `DispatchQueue.main.sync` calls that can deadlock."),
    AuditPatternDefinition(name: "thread-sleep", severity: "warning", description: "Detects blocking `Thread.sleep` usage in application code."),
    AuditPatternDefinition(name: "print-statement", severity: "info", description: "Detects stray `print` debugging statements."),
    AuditPatternDefinition(name: "todo-fixme", severity: "info", description: "Detects TODO and FIXME markers left in source files."),
    AuditPatternDefinition(name: "large-tuple", severity: "warning", description: "Detects large tuple types that hurt readability and maintainability."),
    AuditPatternDefinition(name: "nested-closure", severity: "warning", description: "Detects deeply nested closures that are hard to follow."),
  ]

  public func auditAntiPatterns(
    repoPath: String? = nil,
    patterns: [AuditPatternDefinition]? = nil,
    language: String? = nil,
    limit: Int = 50
  ) async throws -> [String: [AuditMatch]] {
    let selectedPatterns = patterns ?? Self.builtInAuditPatterns
    guard !selectedPatterns.isEmpty else { return [:] }

    let chunks = try loadAuditChunks(repoPath: repoPath, language: language, constructType: nil, requireTags: false)
    var grouped: [String: [AuditMatch]] = [:]
    var totalMatches = 0

    for chunk in chunks {
      for pattern in selectedPatterns {
        let matchCount = Self.matchCount(for: pattern.name, in: chunk.text)
        guard matchCount > 0 else { continue }

        grouped[pattern.name, default: []].append(
          AuditMatch(
            patternName: pattern.name,
            severity: pattern.severity,
            description: pattern.description,
            filePath: chunk.filePath,
            startLine: chunk.startLine,
            endLine: chunk.endLine,
            snippet: chunk.text,
            constructName: chunk.constructName,
            constructType: chunk.constructType,
            matchCount: matchCount
          )
        )
        totalMatches += matchCount
        if totalMatches >= limit {
          return grouped
        }
      }
    }

    return grouped
  }

  public func auditConcurrency(
    repoPath: String? = nil,
    categories: [String]? = nil,
    limit: Int = 50
  ) async throws -> [ConcurrencyFinding] {
    let requestedCategories = categories.flatMap { $0.isEmpty ? nil : Set($0) }
    let chunks = try loadAuditChunks(repoPath: repoPath, language: "swift", constructType: nil, requireTags: false)
    var findings: [ConcurrencyFinding] = []

    for chunk in chunks {
      findings.append(contentsOf: Self.concurrencyFindings(in: chunk, categories: requestedCategories))
      if findings.count >= limit {
        break
      }
    }

    return Array(findings.prefix(limit))
  }

  public func queryByTags(
    tags: [String],
    repoPath: String? = nil,
    constructType: String? = nil,
    matchAll: Bool = false,
    limit: Int = 20
  ) async throws -> [TagQueryResult] {
    let normalizedTags = Set(tags.map { $0.lowercased() })
    guard !normalizedTags.isEmpty else { return [] }

    let chunks = try loadAuditChunks(repoPath: repoPath, language: nil, constructType: constructType, requireTags: true)
    var results: [TagQueryResult] = []

    for chunk in chunks {
      let chunkTags = Set(chunk.aiTags.map { $0.lowercased() })
      let matches = matchAll ? normalizedTags.isSubset(of: chunkTags) : !normalizedTags.isDisjoint(with: chunkTags)
      guard matches else { continue }

      results.append(
        TagQueryResult(
          filePath: chunk.filePath,
          startLine: chunk.startLine,
          endLine: chunk.endLine,
          constructName: chunk.constructName,
          constructType: chunk.constructType,
          snippet: chunk.text,
          aiSummary: chunk.aiSummary,
          aiTags: chunk.aiTags,
          tokenCount: chunk.tokenCount
        )
      )
      if results.count >= limit {
        break
      }
    }

    return results
  }

  public func getTagCounts(repoPath: String? = nil) async throws -> [(tag: String, count: Int)] {
    let chunks = try loadAuditChunks(repoPath: repoPath, language: nil, constructType: nil, requireTags: true)
    var counts: [String: Int] = [:]

    for chunk in chunks {
      for tag in chunk.aiTags {
        counts[tag, default: 0] += 1
      }
    }

    return counts
      .map { (tag: $0.key, count: $0.value) }
      .sorted {
        if $0.count == $1.count {
          return $0.tag < $1.tag
        }
        return $0.count > $1.count
      }
  }

  private struct AuditChunkRow {
    let filePath: String
    let startLine: Int
    let endLine: Int
    let text: String
    let constructType: String?
    let constructName: String?
    let aiSummary: String?
    let aiTags: [String]
    let tokenCount: Int
  }

  private func loadAuditChunks(
    repoPath: String?,
    language: String?,
    constructType: String?,
    requireTags: Bool
  ) throws -> [AuditChunkRow] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    let resolvedRepoId: String? = if let repoPath { try resolveRepoId(for: repoPath) } else { nil }

    var sql = """
      SELECT repos.root_path || '/' || files.path,
             chunks.start_line,
             chunks.end_line,
             chunks.text,
             chunks.construct_type,
             chunks.construct_name,
             chunks.ai_summary,
             chunks.ai_tags,
             chunks.token_count
      FROM chunks
      JOIN files ON files.id = chunks.file_id
      JOIN repos ON repos.id = files.repo_id
      WHERE 1=1
      """

    if resolvedRepoId != nil { sql += " AND repos.id = ?" }
    if language != nil { sql += " AND LOWER(files.language) = ?" }
    if constructType != nil { sql += " AND chunks.construct_type = ?" }
    if requireTags { sql += " AND chunks.ai_tags IS NOT NULL AND chunks.ai_tags != '[]'" }
    sql += " ORDER BY files.path, chunks.start_line"

    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }

    var bindIndex: Int32 = 1
    if let resolvedRepoId {
      bindText(stmt, bindIndex, resolvedRepoId)
      bindIndex += 1
    }
    if let language {
      bindText(stmt, bindIndex, language.lowercased())
      bindIndex += 1
    }
    if let constructType {
      bindText(stmt, bindIndex, constructType)
    }

    var chunks: [AuditChunkRow] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let filePath = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let startLine = Int(sqlite3_column_int(stmt, 1))
      let endLine = Int(sqlite3_column_int(stmt, 2))
      let text = sqlite3_column_text(stmt, 3).map { String(cString: $0) } ?? ""
      let constructType = sqlite3_column_text(stmt, 4).map { String(cString: $0) }
      let constructName = sqlite3_column_text(stmt, 5).map { String(cString: $0) }
      let aiSummary = sqlite3_column_text(stmt, 6).map { String(cString: $0) }
      let aiTagsJson = sqlite3_column_text(stmt, 7).map { String(cString: $0) }
      let tokenCount = Int(sqlite3_column_int(stmt, 8))
      let aiTags = Self.decodeTagArray(aiTagsJson)

      chunks.append(
        AuditChunkRow(
          filePath: filePath,
          startLine: startLine,
          endLine: endLine,
          text: text,
          constructType: constructType,
          constructName: constructName,
          aiSummary: aiSummary,
          aiTags: aiTags,
          tokenCount: tokenCount
        )
      )
    }

    return chunks
  }

  private static func decodeTagArray(_ json: String?) -> [String] {
    guard let json, let data = json.data(using: .utf8) else { return [] }
    return (try? JSONDecoder().decode([String].self, from: data)) ?? []
  }

  private static func matchCount(for patternName: String, in text: String) -> Int {
    switch patternName {
    case "silent-try":
      return regexMatchCount(#"\btry\?"#, in: text)
    case "force-unwrap":
      return regexMatchCount(#"\btry!|[A-Za-z0-9_\)\]]!"#, in: text)
    case "main-sync-dispatch":
      return regexMatchCount(#"DispatchQueue\.main\.sync\s*\{"#, in: text)
    case "thread-sleep":
      return regexMatchCount(#"Thread\.sleep\s*\("#, in: text)
    case "print-statement":
      return regexMatchCount(#"\bprint\s*\("#, in: text)
    case "todo-fixme":
      return regexMatchCount(#"\b(?:TODO|FIXME)\b"#, in: text)
    case "large-tuple":
      return regexMatchCount(#"\((?:[^()\n]*,){3,}[^()\n]*\)"#, in: text)
    case "nested-closure":
      let closures = regexMatchCount(#"\{\s*(?:\[[^\]]*\]\s*)?(?:\([^\)]*\)\s*)?(?:->\s*[^\{=]+\s*)?in\b"#, in: text)
      return closures >= 2 ? closures - 1 : 0
    default:
      return 0
    }
  }

  private static func concurrencyFindings(in chunk: AuditChunkRow, categories: Set<String>?) -> [ConcurrencyFinding] {
    var findings: [ConcurrencyFinding] = []

    func include(_ category: String) -> Bool {
      guard let categories, !categories.isEmpty else { return true }
      return categories.contains(category)
    }

    if include("mainactor-heavyweight"), chunk.text.contains("@MainActor"), chunk.tokenCount >= 700 {
      findings.append(
        ConcurrencyFinding(
          category: "mainactor-heavyweight",
          severity: "error",
          filePath: chunk.filePath,
          startLine: chunk.startLine,
          endLine: chunk.endLine,
          constructName: chunk.constructName,
          constructType: chunk.constructType,
          detail: "Large @MainActor-scoped construct may serialize too much work onto the main actor.",
          snippet: chunk.text
        )
      )
    }

    if include("task-inherits-isolation"), chunk.text.range(of: #"Task\s*\{"#, options: .regularExpression) != nil {
      findings.append(
        ConcurrencyFinding(
          category: "task-inherits-isolation",
          severity: "error",
          filePath: chunk.filePath,
          startLine: chunk.startLine,
          endLine: chunk.endLine,
          constructName: chunk.constructName,
          constructType: chunk.constructType,
          detail: "Unstructured Task inherits current actor isolation and priority; verify detached work is not accidentally running on the caller's actor.",
          snippet: chunk.text
        )
      )
    }

    if include("nonisolated-candidate"),
       (chunk.text.contains("actor ") || chunk.text.contains("@MainActor")),
       chunk.text.range(of: #"\bfunc\b"#, options: .regularExpression) != nil,
       !chunk.text.contains("self."),
       chunk.tokenCount <= 220 {
      findings.append(
        ConcurrencyFinding(
          category: "nonisolated-candidate",
          severity: "warning",
          filePath: chunk.filePath,
          startLine: chunk.startLine,
          endLine: chunk.endLine,
          constructName: chunk.constructName,
          constructType: chunk.constructType,
          detail: "Small isolated function does not appear to touch actor state and may be a candidate for `nonisolated`.",
          snippet: chunk.text
        )
      )
    }

    let broadcastCount = regexMatchCount(#"NotificationCenter\.default\.post\s*\(|objectWillChange\.send\s*\(|subject\.send\s*\("#, in: chunk.text)
    if include("broadcast-amplification"), broadcastCount >= 3 {
      findings.append(
        ConcurrencyFinding(
          category: "broadcast-amplification",
          severity: "warning",
          filePath: chunk.filePath,
          startLine: chunk.startLine,
          endLine: chunk.endLine,
          constructName: chunk.constructName,
          constructType: chunk.constructType,
          detail: "Construct emits multiple broadcast-style updates; verify this does not fan out unnecessary work across observers.",
          snippet: chunk.text
        )
      )
    }

    return findings
  }

  private static func regexMatchCount(_ pattern: String, in text: String) -> Int {
    guard let regex = try? NSRegularExpression(pattern: pattern) else { return 0 }
    let range = NSRange(text.startIndex..<text.endIndex, in: text)
    return regex.numberOfMatches(in: text, options: [], range: range)
  }
}//
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
