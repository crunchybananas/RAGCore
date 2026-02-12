//
//  RAGStore+Lessons.swift
//  RAGCore
//
//  Lessons learned from fixing errors — agent mistake → fix patterns.
//

import CSQLite
import Foundation

extension RAGStore {

  // MARK: - Lesson CRUD

  /// Add a lesson learned from fixing an error.
  public func addLesson(
    repoPath: String,
    filePattern: String? = nil,
    errorSignature: String? = nil,
    fixDescription: String,
    fixCode: String? = nil,
    source: String = "manual"
  ) throws -> RAGLesson {
    try openIfNeeded()
    try ensureSchema()

    let repoId = VectorMath.stableId(for: repoPath)
    let lessonId = VectorMath.stableId(for: "\(repoId):\(fixDescription):\(Date().timeIntervalSince1970)")
    let now = dateFormatter.string(from: Date())

    let sql = """
      INSERT INTO lessons (id, repo_id, error_signature, file_pattern, fix_description, fix_code, source, confidence, is_active, created_at, updated_at, apply_count, success_count)
      VALUES (?, ?, ?, ?, ?, ?, ?, 1.0, 1, ?, ?, 0, 0)
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, lessonId)
      bindText(stmt, 2, repoId)
      bindTextOrNull(stmt, 3, errorSignature)
      bindTextOrNull(stmt, 4, filePattern)
      bindText(stmt, 5, fixDescription)
      bindTextOrNull(stmt, 6, fixCode)
      bindText(stmt, 7, source)
      bindText(stmt, 8, now)
      bindText(stmt, 9, now)
    }

    return RAGLesson(
      id: lessonId, repoId: repoId, errorSignature: errorSignature,
      filePattern: filePattern, fixDescription: fixDescription, fixCode: fixCode,
      source: source, confidence: 1.0, isActive: true,
      createdAt: now, updatedAt: now, applyCount: 0, successCount: 0
    )
  }

  /// Query lessons matching an error signature or file pattern.
  public func queryLessons(
    repoPath: String,
    filePattern: String? = nil,
    errorSignature: String? = nil,
    limit: Int = 10
  ) throws -> [RAGLesson] {
    try openIfNeeded()
    try ensureSchema()

    let repoId = VectorMath.stableId(for: repoPath)

    var sql = """
      SELECT id, repo_id, error_signature, file_pattern, fix_description, fix_code,
             source, confidence, is_active, created_at, updated_at, apply_count, success_count
      FROM lessons
      WHERE repo_id = ? AND is_active = 1
      """
    if errorSignature != nil { sql += " AND error_signature LIKE ?" }
    if filePattern != nil { sql += " AND (file_pattern LIKE ? OR file_pattern IS NULL)" }
    sql += " ORDER BY confidence DESC, success_count DESC LIMIT ?"

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }

    var bindIdx: Int32 = 1
    bindText(stmt, bindIdx, repoId); bindIdx += 1
    if let sig = errorSignature { bindText(stmt, bindIdx, "%\(sig)%"); bindIdx += 1 }
    if let pat = filePattern { bindText(stmt, bindIdx, "%\(pat)%"); bindIdx += 1 }
    sqlite3_bind_int(stmt, bindIdx, Int32(limit))

    var lessons: [RAGLesson] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      lessons.append(parseLesson(from: stmt))
    }
    return lessons
  }

  /// List all lessons for a repo.
  public func listLessons(
    repoPath: String,
    includeInactive: Bool = false,
    limit: Int = 50
  ) throws -> [RAGLesson] {
    try openIfNeeded()
    try ensureSchema()

    let repoId = VectorMath.stableId(for: repoPath)

    var sql = """
      SELECT id, repo_id, error_signature, file_pattern, fix_description, fix_code,
             source, confidence, is_active, created_at, updated_at, apply_count, success_count
      FROM lessons WHERE repo_id = ?
      """
    if !includeInactive { sql += " AND is_active = 1" }
    sql += " ORDER BY updated_at DESC LIMIT ?"

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(stmt) }

    bindText(stmt, 1, repoId)
    sqlite3_bind_int(stmt, 2, Int32(limit))

    var lessons: [RAGLesson] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      lessons.append(parseLesson(from: stmt))
    }
    return lessons
  }

  /// Update a lesson's description, code, confidence, or active status.
  public func updateLesson(
    lessonId: String,
    fixDescription: String? = nil,
    fixCode: String? = nil,
    confidence: Double? = nil,
    isActive: Bool? = nil
  ) throws {
    try openIfNeeded()
    try ensureSchema()

    let now = dateFormatter.string(from: Date())
    var updates: [String] = ["updated_at = ?"]
    if fixDescription != nil { updates.append("fix_description = ?") }
    if fixCode != nil { updates.append("fix_code = ?") }
    if confidence != nil { updates.append("confidence = ?") }
    if isActive != nil { updates.append("is_active = ?") }

    let sql = "UPDATE lessons SET \(updates.joined(separator: ", ")) WHERE id = ?"

    try execute(sql: sql) { stmt in
      var idx: Int32 = 1
      bindText(stmt, idx, now); idx += 1
      if let desc = fixDescription { bindText(stmt, idx, desc); idx += 1 }
      if let code = fixCode { bindText(stmt, idx, code); idx += 1 }
      if let conf = confidence { sqlite3_bind_double(stmt, idx, conf); idx += 1 }
      if let active = isActive { sqlite3_bind_int(stmt, idx, active ? 1 : 0); idx += 1 }
      bindText(stmt, idx, lessonId)
    }
  }

  /// Record that a lesson was applied (success or failure).
  public func recordLessonUsed(lessonId: String, success: Bool) throws {
    try openIfNeeded()
    try ensureSchema()

    let now = dateFormatter.string(from: Date())
    let sql: String
    if success {
      sql = "UPDATE lessons SET apply_count = apply_count + 1, success_count = success_count + 1, updated_at = ? WHERE id = ?"
    } else {
      sql = """
        UPDATE lessons SET apply_count = apply_count + 1,
               confidence = MAX(0.1, confidence - 0.1), updated_at = ?
        WHERE id = ?
        """
    }
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, now)
      bindText(stmt, 2, lessonId)
    }
  }

  /// Delete a lesson.
  public func deleteLesson(lessonId: String) throws {
    try openIfNeeded()
    try ensureSchema()
    try execute(sql: "DELETE FROM lessons WHERE id = ?") { stmt in
      bindText(stmt, 1, lessonId)
    }
  }

  /// Get a single lesson by ID.
  public func getLesson(lessonId: String) throws -> RAGLesson? {
    try openIfNeeded()
    try ensureSchema()

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = """
      SELECT id, repo_id, error_signature, file_pattern, fix_description, fix_code,
             source, confidence, is_active, created_at, updated_at, apply_count, success_count
      FROM lessons WHERE id = ?
      """
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return nil }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, lessonId)
    guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
    return parseLesson(from: stmt)
  }

  // MARK: - Query Hints

  /// Record a query hint for search analytics.
  public func recordQueryHint(query: String, resultCount: Int, searchMode: String) throws {
    try openIfNeeded()
    try ensureSchema()

    let id = VectorMath.stableId(for: "\(query):\(Date().timeIntervalSince1970)")
    let now = dateFormatter.string(from: Date())

    let sql = """
      INSERT INTO rag_query_hints (id, query, result_count, search_mode, created_at)
      VALUES (?, ?, ?, ?, ?)
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, id)
      bindText(stmt, 2, query)
      sqlite3_bind_int(stmt, 3, Int32(resultCount))
      bindText(stmt, 4, searchMode)
      bindText(stmt, 5, now)
    }
  }

  /// Fetch recent query hints.
  public func fetchQueryHints(limit: Int = 20) throws -> [RAGQueryHint] {
    try openIfNeeded()
    try ensureSchema()

    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = "SELECT id, query, result_count, search_mode, created_at FROM rag_query_hints ORDER BY created_at DESC LIMIT ?"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else { return [] }
    defer { sqlite3_finalize(stmt) }
    sqlite3_bind_int(stmt, 1, Int32(limit))

    var hints: [RAGQueryHint] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let id = String(cString: sqlite3_column_text(stmt, 0))
      let query = String(cString: sqlite3_column_text(stmt, 1))
      let resultCount = Int(sqlite3_column_int(stmt, 2))
      let searchMode = String(cString: sqlite3_column_text(stmt, 3))
      let createdAt = String(cString: sqlite3_column_text(stmt, 4))
      hints.append(RAGQueryHint(id: id, query: query, resultCount: resultCount, searchMode: searchMode, createdAt: createdAt))
    }
    return hints
  }

  // MARK: - Parsing Helper

  private func parseLesson(from stmt: OpaquePointer) -> RAGLesson {
    let id = String(cString: sqlite3_column_text(stmt, 0))
    let repoId = String(cString: sqlite3_column_text(stmt, 1))
    let errorSig = sqlite3_column_text(stmt, 2).map { String(cString: $0) }
    let filePattern = sqlite3_column_text(stmt, 3).map { String(cString: $0) }
    let fixDesc = String(cString: sqlite3_column_text(stmt, 4))
    let fixCode = sqlite3_column_text(stmt, 5).map { String(cString: $0) }
    let source = String(cString: sqlite3_column_text(stmt, 6))
    let confidence = sqlite3_column_double(stmt, 7)
    let isActive = sqlite3_column_int(stmt, 8) != 0
    let createdAt = String(cString: sqlite3_column_text(stmt, 9))
    let updatedAt = String(cString: sqlite3_column_text(stmt, 10))
    let applyCount = Int(sqlite3_column_int(stmt, 11))
    let successCount = Int(sqlite3_column_int(stmt, 12))

    return RAGLesson(
      id: id, repoId: repoId, errorSignature: errorSig,
      filePattern: filePattern, fixDescription: fixDesc, fixCode: fixCode,
      source: source, confidence: confidence, isActive: isActive,
      createdAt: createdAt, updatedAt: updatedAt,
      applyCount: applyCount, successCount: successCount
    )
  }
}
