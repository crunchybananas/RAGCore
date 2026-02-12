//
//  RAGStore+Queries.swift
//  RAGCore
//
//  Structural queries, orphan detection, and similar code search.
//

import CSQLite
import Foundation

// MARK: - Result Types

/// Result for orphan detection queries.
public struct RAGOrphanResult: Sendable {
  public let filePath: String
  public let language: String
  public let lineCount: Int
  public let symbolsDefinedCount: Int
  public let symbolsDefined: [String]
  public let reason: String
}

/// Result for structural queries.
public struct RAGStructuralResult: Sendable {
  public let path: String
  public let language: String
  public let lineCount: Int
  public let methodCount: Int
  public let byteSize: Int
  public let modulePath: String?
}

/// Structural statistics for a repository.
public struct RAGStructuralStats: Sendable {
  public let totalFiles: Int
  public let totalLines: Int
  public let totalMethods: Int
  public let avgLinesPerFile: Double
  public let avgMethodsPerFile: Double
  public let largestFile: (path: String, lines: Int)?
  public let mostMethods: (path: String, count: Int)?
}

/// Result for similar code queries.
public struct RAGSimilarResult: Sendable {
  public let path: String
  public let startLine: Int
  public let endLine: Int
  public let snippet: String
  public let similarity: Double
  public let constructType: String?
  public let constructName: String?
}

// MARK: - Orphan Detection

extension RAGStore {

  /// Find potentially orphaned files (files with no dependents).
  ///
  /// An orphan is a file that:
  /// 1. Is not imported/required by any other file
  /// 2. Has no symbol_refs from other files pointing to its defined types
  /// 3. Is not a test file (optional exclusion)
  /// 4. Is not an entry point (optional exclusion)
  public func findOrphans(
    repoPath: String,
    excludeTests: Bool = true,
    excludeEntryPoints: Bool = true,
    limit: Int = 50
  ) throws -> [RAGOrphanResult] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    // Get the repo ID
    let repoIdSql = "SELECT id FROM repos WHERE root_path = ?"
    var statement: OpaquePointer?
    var result = sqlite3_prepare_v2(db, repoIdSql, -1, &statement, nil)
    guard result == SQLITE_OK, let stmt = statement else {
      throw RAGError.sqlite("Failed to prepare repo query")
    }
    bindText(stmt, 1, repoPath)
    var repoId: String?
    if sqlite3_step(stmt) == SQLITE_ROW, let text = sqlite3_column_text(stmt, 0) {
      repoId = String(cString: text)
    }
    sqlite3_finalize(stmt)

    guard let repoId else { return [] }

    // Build exclusion patterns
    var excludePatterns: [String] = []
    if excludeTests {
      excludePatterns.append(contentsOf: [
        "%Test.swift", "%Tests.swift", "%Spec.swift",
        "%_test.swift", "%_tests.swift", "%_spec.swift",
        "%Test.ts", "%Spec.ts", "%_test.ts", "%_spec.ts",
        "Tests/%", "Test/%", "test/%", "__tests__/%", "spec/%",
        "%_test.rb", "%_spec.rb",
      ])
    }
    if excludeEntryPoints {
      excludePatterns.append(contentsOf: [
        "%App.swift", "main.swift", "%Main.swift",
        "PeelApp.swift", "ContentView.swift",
        "index.ts", "index.js", "main.ts", "main.js",
        "application.rb", "routes.rb",
      ])
    }

    var sql = """
      SELECT
        f.path,
        f.language,
        f.line_count,
        (SELECT COUNT(*) FROM symbols WHERE file_id = f.id) as symbols_defined,
        (SELECT GROUP_CONCAT(name, ', ') FROM (SELECT name FROM symbols WHERE file_id = f.id LIMIT 5)) as symbol_names
      FROM files f
      WHERE f.repo_id = ?
        AND NOT EXISTS (
          SELECT 1 FROM dependencies d WHERE d.target_file_id = f.id
        )
        AND NOT EXISTS (
          SELECT 1 FROM symbol_refs sr
          JOIN symbols s ON s.name = sr.referenced_name AND s.repo_id = f.repo_id
          WHERE s.file_id = f.id AND sr.source_file_id != f.id
        )
      """
    for pattern in excludePatterns {
      sql += "\n    AND f.path NOT LIKE '\(pattern)'"
    }
    sql += "\n  ORDER BY f.line_count DESC LIMIT \(limit)"

    result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let stmt2 = statement else {
      let errMsg = String(cString: sqlite3_errmsg(db))
      throw RAGError.sqlite("Failed to prepare orphan query: \(errMsg)")
    }
    defer { sqlite3_finalize(stmt2) }
    bindText(stmt2, 1, repoId)

    var orphans: [RAGOrphanResult] = []
    while sqlite3_step(stmt2) == SQLITE_ROW {
      let path = sqlite3_column_text(stmt2, 0).map { String(cString: $0) } ?? ""
      let language = sqlite3_column_text(stmt2, 1).map { String(cString: $0) } ?? ""
      let lineCount = Int(sqlite3_column_int(stmt2, 2))
      let symbolCount = Int(sqlite3_column_int(stmt2, 3))
      let symbolNames = sqlite3_column_text(stmt2, 4).map { String(cString: $0) } ?? ""
      let symbols = symbolNames.components(separatedBy: ", ").filter { !$0.isEmpty }

      orphans.append(RAGOrphanResult(
        filePath: path,
        language: language,
        lineCount: lineCount,
        symbolsDefinedCount: symbolCount,
        symbolsDefined: symbols,
        reason: "No imports or type references from other files"
      ))
    }
    return orphans
  }
}

// MARK: - Structural Queries

extension RAGStore {

  /// Query files by structural characteristics (line count, method count, file size).
  public func queryFilesByStructure(
    inRepo repoPath: String,
    minLines: Int? = nil,
    maxLines: Int? = nil,
    minMethods: Int? = nil,
    maxMethods: Int? = nil,
    minBytes: Int? = nil,
    maxBytes: Int? = nil,
    language: String? = nil,
    sortBy: String = "lines",
    limit: Int = 50
  ) throws -> [RAGStructuralResult] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    var conditions = ["repos.root_path = ?"]
    var params: [Any] = [repoPath]

    if let minLines { conditions.append("files.line_count >= ?"); params.append(minLines) }
    if let maxLines { conditions.append("files.line_count <= ?"); params.append(maxLines) }
    if let minMethods { conditions.append("files.method_count >= ?"); params.append(minMethods) }
    if let maxMethods { conditions.append("files.method_count <= ?"); params.append(maxMethods) }
    if let minBytes { conditions.append("files.byte_size >= ?"); params.append(minBytes) }
    if let maxBytes { conditions.append("files.byte_size <= ?"); params.append(maxBytes) }
    if let language, !language.isEmpty { conditions.append("files.language = ?"); params.append(language) }

    let sortColumn: String
    switch sortBy.lowercased() {
    case "methods": sortColumn = "files.method_count"
    case "bytes", "size": sortColumn = "files.byte_size"
    default: sortColumn = "files.line_count"
    }

    let sql = """
      SELECT files.path, files.language,
             COALESCE(files.line_count, 0) as line_count,
             COALESCE(files.method_count, 0) as method_count,
             COALESCE(files.byte_size, 0) as byte_size,
             files.module_path
      FROM files
      JOIN repos ON repos.id = files.repo_id
      WHERE \(conditions.joined(separator: " AND "))
      ORDER BY \(sortColumn) DESC
      LIMIT ?
      """

    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let stmt = statement else {
      throw RAGError.sqlite("Failed to prepare structural query")
    }
    defer { sqlite3_finalize(stmt) }

    var paramIndex: Int32 = 1
    for param in params {
      if let str = param as? String {
        bindText(stmt, paramIndex, str)
      } else if let int = param as? Int {
        sqlite3_bind_int(stmt, paramIndex, Int32(int))
      }
      paramIndex += 1
    }
    sqlite3_bind_int(stmt, paramIndex, Int32(limit))

    var results: [RAGStructuralResult] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let path = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let lang = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
      let lines = Int(sqlite3_column_int(stmt, 2))
      let methods = Int(sqlite3_column_int(stmt, 3))
      let bytes = Int(sqlite3_column_int(stmt, 4))
      let modulePath = sqlite3_column_text(stmt, 5).map { String(cString: $0) }
      results.append(RAGStructuralResult(path: path, language: lang, lineCount: lines, methodCount: methods, byteSize: bytes, modulePath: modulePath))
    }
    return results
  }

  /// Get structural statistics for a repository.
  public func getStructuralStats(for repoPath: String) throws -> RAGStructuralStats {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    let statsSql = """
      SELECT COUNT(*) as file_count,
             COALESCE(SUM(line_count), 0) as total_lines,
             COALESCE(SUM(method_count), 0) as total_methods
      FROM files JOIN repos ON repos.id = files.repo_id
      WHERE repos.root_path = ?
      """
    var statement: OpaquePointer?
    var result = sqlite3_prepare_v2(db, statsSql, -1, &statement, nil)
    guard result == SQLITE_OK, let stmt = statement else {
      throw RAGError.sqlite("Failed to prepare stats query")
    }
    bindText(stmt, 1, repoPath)

    var totalFiles = 0, totalLines = 0, totalMethods = 0
    if sqlite3_step(stmt) == SQLITE_ROW {
      totalFiles = Int(sqlite3_column_int(stmt, 0))
      totalLines = Int(sqlite3_column_int(stmt, 1))
      totalMethods = Int(sqlite3_column_int(stmt, 2))
    }
    sqlite3_finalize(stmt)

    // Largest file by lines
    let largestSql = """
      SELECT files.path, COALESCE(files.line_count, 0) as lines
      FROM files JOIN repos ON repos.id = files.repo_id
      WHERE repos.root_path = ?
      ORDER BY lines DESC LIMIT 1
      """
    result = sqlite3_prepare_v2(db, largestSql, -1, &statement, nil)
    var largestFile: (path: String, lines: Int)?
    if result == SQLITE_OK, let stmt2 = statement {
      bindText(stmt2, 1, repoPath)
      if sqlite3_step(stmt2) == SQLITE_ROW {
        let path = sqlite3_column_text(stmt2, 0).map { String(cString: $0) } ?? ""
        let lines = Int(sqlite3_column_int(stmt2, 1))
        if lines > 0 { largestFile = (path, lines) }
      }
      sqlite3_finalize(stmt2)
    }

    // Most methods
    let mostMethodsSql = """
      SELECT files.path, COALESCE(files.method_count, 0) as methods
      FROM files JOIN repos ON repos.id = files.repo_id
      WHERE repos.root_path = ?
      ORDER BY methods DESC LIMIT 1
      """
    result = sqlite3_prepare_v2(db, mostMethodsSql, -1, &statement, nil)
    var mostMethods: (path: String, count: Int)?
    if result == SQLITE_OK, let stmt3 = statement {
      bindText(stmt3, 1, repoPath)
      if sqlite3_step(stmt3) == SQLITE_ROW {
        let path = sqlite3_column_text(stmt3, 0).map { String(cString: $0) } ?? ""
        let count = Int(sqlite3_column_int(stmt3, 1))
        if count > 0 { mostMethods = (path, count) }
      }
      sqlite3_finalize(stmt3)
    }

    let avgLines = totalFiles > 0 ? Double(totalLines) / Double(totalFiles) : 0
    let avgMethods = totalFiles > 0 ? Double(totalMethods) / Double(totalFiles) : 0

    return RAGStructuralStats(
      totalFiles: totalFiles, totalLines: totalLines, totalMethods: totalMethods,
      avgLinesPerFile: avgLines, avgMethodsPerFile: avgMethods,
      largestFile: largestFile, mostMethods: mostMethods
    )
  }
}

// MARK: - Similar Code Detection

extension RAGStore {

  /// Find code chunks semantically similar to a given query.
  public func findSimilarCode(
    query: String,
    repoPath: String? = nil,
    threshold: Double = 0.6,
    limit: Int = 10,
    excludePath: String? = nil
  ) async throws -> [RAGSimilarResult] {
    try openIfNeeded()
    guard db != nil else { throw RAGError.sqlite("Database not initialized") }

    let embeddings = try await generateEmbeddings(for: [query])
    guard let queryEmbedding = embeddings.first, !queryEmbedding.isEmpty else {
      throw RAGError.embeddingFailed("Failed to generate embedding for query")
    }

    if extensionLoaded {
      return try findSimilarCodeAccelerated(
        queryVector: queryEmbedding, repoPath: repoPath,
        threshold: threshold, limit: limit, excludePath: excludePath
      )
    } else {
      return try findSimilarCodeBruteForce(
        queryVector: queryEmbedding, repoPath: repoPath,
        threshold: threshold, limit: limit, excludePath: excludePath
      )
    }
  }

  /// Accelerated similar code search using sqlite-vec.
  private func findSimilarCodeAccelerated(
    queryVector: [Float],
    repoPath: String?,
    threshold: Double,
    limit: Int,
    excludePath: String?
  ) throws -> [RAGSimilarResult] {
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    let sql = """
      SELECT
        c.id, f.path, c.start_line, c.end_line, c.text,
        c.construct_type, c.construct_name,
        vec_distance_cosine(v.embedding, ?) as distance
      FROM vec_chunks v
      JOIN chunks c ON c.id = v.chunk_id
      JOIN files f ON f.id = c.file_id
      JOIN repos r ON r.id = f.repo_id
      WHERE (\(repoPath == nil ? "1=1" : "r.root_path = ?"))
        \(excludePath != nil ? "AND f.path != ?" : "")
      ORDER BY distance ASC
      LIMIT ?
      """

    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let stmt = statement else {
      throw RAGError.sqlite("Failed to prepare similar code query")
    }
    defer { sqlite3_finalize(stmt) }

    let vectorData = queryVector.withUnsafeBytes { Data($0) }
    vectorData.withUnsafeBytes { ptr in
      sqlite3_bind_blob(stmt, 1, ptr.baseAddress, Int32(vectorData.count), nil)
    }

    var paramIndex: Int32 = 2
    if let repoPath { bindText(stmt, paramIndex, repoPath); paramIndex += 1 }
    if let excludePath { bindText(stmt, paramIndex, excludePath); paramIndex += 1 }
    sqlite3_bind_int(stmt, paramIndex, Int32(limit * 2))

    var results: [RAGSimilarResult] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let path = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
      let startLine = Int(sqlite3_column_int(stmt, 2))
      let endLine = Int(sqlite3_column_int(stmt, 3))
      let snippet = sqlite3_column_text(stmt, 4).map { String(cString: $0) } ?? ""
      let constructType = sqlite3_column_text(stmt, 5).map { String(cString: $0) }
      let constructName = sqlite3_column_text(stmt, 6).map { String(cString: $0) }
      let distance = sqlite3_column_double(stmt, 7)
      let similarity = max(0.0, 1.0 - distance)

      guard similarity >= threshold else { continue }
      results.append(RAGSimilarResult(
        path: path, startLine: startLine, endLine: endLine, snippet: snippet,
        similarity: similarity, constructType: constructType, constructName: constructName
      ))
      if results.count >= limit { break }
    }
    return results
  }

  /// Brute-force similar code search using cosine similarity.
  private func findSimilarCodeBruteForce(
    queryVector: [Float],
    repoPath: String?,
    threshold: Double,
    limit: Int,
    excludePath: String?
  ) throws -> [RAGSimilarResult] {
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    var sql = """
      SELECT e.embedding, c.id, f.path, c.start_line, c.end_line,
             c.text, c.construct_type, c.construct_name
      FROM embeddings e
      JOIN chunks c ON c.id = e.chunk_id
      JOIN files f ON f.id = c.file_id
      JOIN repos r ON r.id = f.repo_id
      WHERE 1=1
      """
    if repoPath != nil { sql += " AND r.root_path = ?" }
    if excludePath != nil { sql += " AND f.path != ?" }

    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let stmt = statement else {
      let errMsg = String(cString: sqlite3_errmsg(db))
      throw RAGError.sqlite("Failed to prepare brute-force similarity query: \(errMsg)")
    }
    defer { sqlite3_finalize(stmt) }

    var paramIndex: Int32 = 1
    if let repoPath { bindText(stmt, paramIndex, repoPath); paramIndex += 1 }
    if let excludePath { bindText(stmt, paramIndex, excludePath) }

    var candidates: [(path: String, startLine: Int, endLine: Int, snippet: String, similarity: Double, constructType: String?, constructName: String?)] = []

    while sqlite3_step(stmt) == SQLITE_ROW {
      guard let blob = sqlite3_column_blob(stmt, 0) else { continue }
      let bytes = sqlite3_column_bytes(stmt, 0)
      let floatCount = Int(bytes) / MemoryLayout<Float>.size
      let embeddingVector = Array(UnsafeBufferPointer(
        start: blob.assumingMemoryBound(to: Float.self), count: floatCount
      ))

      let similarity = Double(VectorMath.cosineSimilarity(queryVector, embeddingVector))
      guard similarity >= threshold else { continue }

      let path = sqlite3_column_text(stmt, 2).map { String(cString: $0) } ?? ""
      let startLine = Int(sqlite3_column_int(stmt, 3))
      let endLine = Int(sqlite3_column_int(stmt, 4))
      let snippet = sqlite3_column_text(stmt, 5).map { String(cString: $0) } ?? ""
      let constructType = sqlite3_column_text(stmt, 6).map { String(cString: $0) }
      let constructName = sqlite3_column_text(stmt, 7).map { String(cString: $0) }
      candidates.append((path, startLine, endLine, snippet, similarity, constructType, constructName))
    }

    candidates.sort { $0.similarity > $1.similarity }
    return candidates.prefix(limit).map { c in
      RAGSimilarResult(
        path: c.path, startLine: c.startLine, endLine: c.endLine,
        snippet: c.snippet, similarity: c.similarity,
        constructType: c.constructType, constructName: c.constructName
      )
    }
  }
}
