//
//  RAGStore+Dependencies.swift
//  RAGCore
//
//  Dependency graph: import tracking, symbol references, orphan detection.
//

import ASTChunker
import CSQLite
import Foundation

extension RAGStore {

  // MARK: - Dependency Extraction

  /// Symbol reference model for intra-file type usage.
  struct SymbolRef {
    let id: String
    let repoId: String
    let sourceFileId: String
    let referencedName: String
    let refKind: String  // "type", "conform", "inherit", "mixin"
  }

  /// Extract import/require dependencies from chunk metadata.
  internal func extractDependencies(
    from chunks: [RAGChunk],
    repoId: String,
    fileId: String,
    relativePath: String,
    language: String
  ) -> [RAGDependency] {
    var deps: [RAGDependency] = []
    var seenImports = Set<String>()

    for chunk in chunks {
      guard let metadataJson = chunk.metadata,
            let data = metadataJson.data(using: .utf8),
            let metadata = try? JSONDecoder().decode(ASTChunkMetadata.self, from: data) else {
        continue
      }

      for importPath in metadata.imports {
        guard !seenImports.contains(importPath) else { continue }
        seenImports.insert(importPath)

        let depType = determineImportType(importPath, language: language)
        let targetFileId = resolveTargetFile(targetPath: importPath, inRepo: repoId, fromFile: relativePath)

        let id = VectorMath.stableId(for: "\(fileId):\(importPath)")
        deps.append(RAGDependency(
          id: id,
          repoId: repoId,
          sourceFileId: fileId,
          targetPath: importPath,
          targetFileId: targetFileId,
          dependencyType: depType,
          rawImport: importPath
        ))
      }

      // Superclass as inheritance dependency
      if let superclass = metadata.superclass, !seenImports.contains("inherit:\(superclass)") {
        seenImports.insert("inherit:\(superclass)")
        let id = VectorMath.stableId(for: "\(fileId):inherit:\(superclass)")
        deps.append(RAGDependency(
          id: id, repoId: repoId, sourceFileId: fileId,
          targetPath: superclass, targetFileId: nil,
          dependencyType: .inherit, rawImport: superclass
        ))
      }

      // Protocols as conformance dependencies
      for proto in metadata.protocols {
        guard !seenImports.contains("conform:\(proto)") else { continue }
        seenImports.insert("conform:\(proto)")
        let id = VectorMath.stableId(for: "\(fileId):conform:\(proto)")
        deps.append(RAGDependency(
          id: id, repoId: repoId, sourceFileId: fileId,
          targetPath: proto, targetFileId: nil,
          dependencyType: .conform, rawImport: proto
        ))
      }

      // Mixins
      for mixin in metadata.mixins {
        guard !seenImports.contains("mixin:\(mixin)") else { continue }
        seenImports.insert("mixin:\(mixin)")
        let id = VectorMath.stableId(for: "\(fileId):mixin:\(mixin)")
        deps.append(RAGDependency(
          id: id, repoId: repoId, sourceFileId: fileId,
          targetPath: mixin, targetFileId: nil,
          dependencyType: .mixin, rawImport: mixin
        ))
      }

      // Frameworks
      for framework in metadata.frameworks ?? [] {
        guard !seenImports.contains("framework:\(framework)") else { continue }
        seenImports.insert("framework:\(framework)")
        let id = VectorMath.stableId(for: "\(fileId):framework:\(framework)")
        deps.append(RAGDependency(
          id: id, repoId: repoId, sourceFileId: fileId,
          targetPath: framework, targetFileId: nil,
          dependencyType: .framework, rawImport: framework
        ))
      }
    }

    return deps
  }

  /// Determine the dependency type from the import string.
  internal func determineImportType(_ importPath: String, language: String) -> RAGDependencyType {
    switch language.lowercased() {
    case "swift":
      return .framework
    case "ruby":
      if importPath.contains("::") { return .inherit }
      return .require
    case "typescript", "javascript":
      if importPath.hasPrefix("./") || importPath.hasPrefix("../") { return .import }
      return .import
    default:
      return .import
    }
  }

  // MARK: - Dependency CRUD

  internal func insertDependency(_ dep: RAGDependency) throws {
    let sql = """
      INSERT OR REPLACE INTO dependencies
        (id, repo_id, source_file_id, target_path, target_file_id, dependency_type, raw_import)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, dep.id)
      bindText(stmt, 2, dep.repoId)
      bindText(stmt, 3, dep.sourceFileId)
      bindText(stmt, 4, dep.targetPath)
      bindTextOrNull(stmt, 5, dep.targetFileId)
      bindText(stmt, 6, dep.dependencyType.rawValue)
      bindTextOrNull(stmt, 7, dep.rawImport)
    }
  }

  internal func insertDependencies(_ deps: [RAGDependency]) throws {
    guard !deps.isEmpty else { return }
    try exec("BEGIN TRANSACTION")
    do {
      for dep in deps { try insertDependency(dep) }
      try exec("COMMIT")
    } catch {
      try? exec("ROLLBACK")
      throw error
    }
  }

  internal func deleteDependencies(for fileId: String) throws {
    try execute(sql: "DELETE FROM dependencies WHERE source_file_id = ?") { stmt in
      bindText(stmt, 1, fileId)
    }
  }

  // MARK: - Symbol References

  /// Extract type references from chunk metadata.
  internal func extractSymbolRefs(
    from chunks: [RAGChunk],
    repoId: String,
    fileId: String
  ) -> [SymbolRef] {
    var refs: [SymbolRef] = []
    var seenNames = Set<String>()

    for chunk in chunks {
      guard let metadataJson = chunk.metadata,
            let data = metadataJson.data(using: .utf8),
            let metadata = try? JSONDecoder().decode(ASTChunkMetadata.self, from: data) else {
        continue
      }

      for typeName in metadata.typeReferences {
        guard !seenNames.contains(typeName) else { continue }
        seenNames.insert(typeName)
        refs.append(SymbolRef(
          id: VectorMath.stableId(for: "\(fileId):\(typeName)"),
          repoId: repoId, sourceFileId: fileId,
          referencedName: typeName, refKind: "type"
        ))
      }

      for proto in metadata.protocols {
        guard !seenNames.contains(proto) else { continue }
        seenNames.insert(proto)
        refs.append(SymbolRef(
          id: VectorMath.stableId(for: "\(fileId):\(proto)"),
          repoId: repoId, sourceFileId: fileId,
          referencedName: proto, refKind: "conform"
        ))
      }

      if let superclass = metadata.superclass, !seenNames.contains(superclass) {
        seenNames.insert(superclass)
        refs.append(SymbolRef(
          id: VectorMath.stableId(for: "\(fileId):\(superclass)"),
          repoId: repoId, sourceFileId: fileId,
          referencedName: superclass, refKind: "inherit"
        ))
      }

      for mixin in metadata.mixins {
        guard !seenNames.contains(mixin) else { continue }
        seenNames.insert(mixin)
        refs.append(SymbolRef(
          id: VectorMath.stableId(for: "\(fileId):\(mixin)"),
          repoId: repoId, sourceFileId: fileId,
          referencedName: mixin, refKind: "mixin"
        ))
      }
    }

    return refs
  }

  internal func insertSymbolRef(_ ref: SymbolRef) throws {
    let sql = """
      INSERT OR REPLACE INTO symbol_refs
        (id, repo_id, source_file_id, referenced_name, ref_kind)
      VALUES (?, ?, ?, ?, ?)
      """
    try execute(sql: sql) { stmt in
      bindText(stmt, 1, ref.id)
      bindText(stmt, 2, ref.repoId)
      bindText(stmt, 3, ref.sourceFileId)
      bindText(stmt, 4, ref.referencedName)
      bindText(stmt, 5, ref.refKind)
    }
  }

  internal func insertSymbolRefs(_ refs: [SymbolRef]) throws {
    guard !refs.isEmpty else { return }
    try exec("BEGIN TRANSACTION")
    do {
      for ref in refs {
        do { try insertSymbolRef(ref) } catch {
          print("[RAG] FK error inserting symbol ref: \(ref.referencedName) — \(error)")
          continue
        }
      }
      try exec("COMMIT")
    } catch {
      try? exec("ROLLBACK")
      throw error
    }
  }

  internal func deleteSymbolRefs(for fileId: String) throws {
    try execute(sql: "DELETE FROM symbol_refs WHERE source_file_id = ?") { stmt in
      bindText(stmt, 1, fileId)
    }
  }

  internal func deleteSymbolRefs(forRepo repoId: String) throws {
    try execute(sql: "DELETE FROM symbol_refs WHERE repo_id = ?") { stmt in
      bindText(stmt, 1, repoId)
    }
  }

  // MARK: - Dependency Queries

  /// Get forward dependencies for a file.
  public func getDependencies(for filePath: String, inRepo repoPath: String) throws -> [RAGDependencyResult] {
    try openIfNeeded()
    let resolvedRepoId = try resolveRepoId(for: repoPath)
    let sql = """
      SELECT f.path, d.target_path, tf.path, d.dependency_type, d.raw_import
      FROM dependencies d
      JOIN files f ON f.id = d.source_file_id
      JOIN repos r ON r.id = d.repo_id
      LEFT JOIN files tf ON tf.id = d.target_file_id
      WHERE r.id = ? AND (f.path = ? OR f.path LIKE ?)
      ORDER BY d.dependency_type, d.target_path
      """
    return try queryDeps(sql: sql) { stmt in
      bindText(stmt, 1, resolvedRepoId)
      bindText(stmt, 2, filePath)
      bindText(stmt, 3, "%/\(filePath)")
    }
  }

  /// Get reverse dependencies (what depends on a file).
  public func getDependents(for filePath: String, inRepo repoPath: String) throws -> [RAGDependencyResult] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let resolvedRepoId = try resolveRepoId(for: repoPath)

    // Find target file ID
    let fidSql = "SELECT f.id FROM files f JOIN repos r ON r.id = f.repo_id WHERE f.path = ? AND r.id = ?"
    var fidStmt: OpaquePointer?
    var targetFileId: String?
    if sqlite3_prepare_v2(db, fidSql, -1, &fidStmt, nil) == SQLITE_OK, let s = fidStmt {
      defer { sqlite3_finalize(s) }
      bindText(s, 1, filePath)
      bindText(s, 2, resolvedRepoId)
      if sqlite3_step(s) == SQLITE_ROW, let t = sqlite3_column_text(s, 0) {
        targetFileId = String(cString: t)
      }
    }

    let sql = """
      SELECT sf.path, d.target_path, tf.path, d.dependency_type, d.raw_import
      FROM dependencies d
      JOIN files sf ON sf.id = d.source_file_id
      JOIN repos r ON r.id = d.repo_id
      LEFT JOIN files tf ON tf.id = d.target_file_id
      WHERE r.id = ? AND (d.target_file_id = ? OR d.target_path = ? OR d.target_path LIKE ?)
      ORDER BY sf.path
      """
    return try queryDeps(sql: sql) { stmt in
      bindText(stmt, 1, resolvedRepoId)
      bindText(stmt, 2, targetFileId ?? "")
      bindText(stmt, 3, filePath)
      bindText(stmt, 4, "%/\(filePath)")
    }
  }

  /// Get all dependency edges for a repo.
  public func getDependencyEdges(for repoPath: String) throws -> [RAGDependencyResult] {
    try openIfNeeded()
    let resolvedRepoId = try resolveRepoId(for: repoPath)
    let sql = """
      SELECT sf.path, d.target_path, tf.path, d.dependency_type, d.raw_import
      FROM dependencies d
      JOIN files sf ON sf.id = d.source_file_id
      JOIN repos r ON r.id = d.repo_id
      LEFT JOIN files tf ON tf.id = d.target_file_id
      WHERE r.id = ? ORDER BY sf.path
      """
    return try queryDeps(sql: sql) { stmt in bindText(stmt, 1, resolvedRepoId) }
  }

  /// Get file summaries for graph aggregation.
  public func getFileSummaries(for repoPath: String) throws -> [RAGFileSummary] {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let resolvedRepoId = try resolveRepoId(for: repoPath)
    let sql = "SELECT f.path, f.language, f.module_path FROM files f JOIN repos r ON r.id = f.repo_id WHERE r.id = ? ORDER BY f.path"
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { throw RAGError.sqlite("Failed to prepare") }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, resolvedRepoId)

    var files: [RAGFileSummary] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let path = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let lang = sqlite3_column_text(stmt, 1).map { String(cString: $0) }
      let mp = sqlite3_column_text(stmt, 2).map { String(cString: $0) }
      files.append(RAGFileSummary(path: path, language: lang, modulePath: mp))
    }
    return files
  }

  /// Get dependency statistics for a repo.
  public func getDependencyStats(for repoPath: String) throws -> (totalDeps: Int, byType: [String: Int]) {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let resolvedRepoId = try resolveRepoId(for: repoPath)
    let sql = """
      SELECT d.dependency_type, COUNT(*) FROM dependencies d
      JOIN repos r ON r.id = d.repo_id WHERE r.id = ?
      GROUP BY d.dependency_type
      """
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { throw RAGError.sqlite("Failed to prepare") }
    defer { sqlite3_finalize(stmt) }
    bindText(stmt, 1, resolvedRepoId)

    var byType: [String: Int] = [:]
    var total = 0
    while sqlite3_step(stmt) == SQLITE_ROW {
      if let t = sqlite3_column_text(stmt, 0) {
        let name = String(cString: t)
        let count = Int(sqlite3_column_int(stmt, 1))
        byType[name] = count
        total += count
      }
    }
    return (total, byType)
  }

  // MARK: - Target Resolution

  /// Resolve an import path to a file ID within the repo.
  internal func resolveTargetFile(targetPath: String, inRepo repoId: String, fromFile sourceFile: String) -> String? {
    let candidates = generateResolutionCandidates(targetPath: targetPath, sourceFile: sourceFile)
    for candidate in candidates {
      if let fileId = try? findFileByPath(candidate, inRepo: repoId) {
        return fileId
      }
    }
    return nil
  }

  private func generateResolutionCandidates(targetPath: String, sourceFile: String) -> [String] {
    var candidates: [String] = []
    let sourceDir = (sourceFile as NSString).deletingLastPathComponent

    if targetPath.hasPrefix("./") || targetPath.hasPrefix("../") {
      var relativePath = targetPath
      while relativePath.hasPrefix("../") { relativePath = String(relativePath.dropFirst(3)) }
      relativePath = relativePath.replacingOccurrences(of: "./", with: "")
      let resolved = (sourceDir as NSString).appendingPathComponent(relativePath)
      candidates.append(resolved)
      for ext in ["", ".swift", ".ts", ".tsx", ".js", ".jsx", ".rb", ".py"] {
        candidates.append(resolved + ext)
      }
    } else {
      candidates.append(targetPath)
      let prefixes = ["", "src/", "lib/", "app/", "Shared/", "Sources/"]
      for prefix in prefixes {
        for ext in ["", ".swift", ".ts", ".tsx", ".js", ".jsx", ".rb", ".py", "/index.ts", "/index.js", "/index.tsx"] {
          candidates.append(prefix + targetPath + ext)
        }
      }
      if targetPath.contains("::") {
        let rubyPath = targetPath.replacingOccurrences(of: "::", with: "/").lowercased()
        for prefix in ["", "app/models/", "app/services/", "lib/"] {
          candidates.append(prefix + rubyPath + ".rb")
        }
      }
      if targetPath.contains("/") {
        let parts = targetPath.split(separator: "/", maxSplits: 1)
        if parts.count == 2 {
          let pkgName = String(parts[0])
          let modPath = String(parts[1])
          for addonPrefix in ["addons/\(pkgName)/src/", "addons/\(pkgName)/"] {
            for ext in ["", ".gts", ".gjs", ".ts", ".js", ".tsx", ".jsx", "/index.gts", "/index.ts", "/index.js"] {
              candidates.append(addonPrefix + modPath + ext)
            }
          }
          for ext in ["", ".gts", ".gjs", ".ts", ".js", ".tsx", ".jsx", "/index.gts", "/index.ts", "/index.js"] {
            candidates.append("\(pkgName)/app/" + modPath + ext)
          }
        }
      }
    }
    return candidates
  }

  private func findFileByPath(_ path: String, inRepo repoId: String) throws -> String? {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    // Exact match
    let exactSql = "SELECT id FROM files WHERE path = ? AND repo_id = ?"
    var stmt: OpaquePointer?
    if sqlite3_prepare_v2(db, exactSql, -1, &stmt, nil) == SQLITE_OK, let s = stmt {
      defer { sqlite3_finalize(s) }
      bindText(s, 1, path); bindText(s, 2, repoId)
      if sqlite3_step(s) == SQLITE_ROW, let t = sqlite3_column_text(s, 0) {
        return String(cString: t)
      }
    }

    // Suffix match
    let suffSql = "SELECT id FROM files WHERE (path LIKE ? OR path LIKE ?) AND repo_id = ? LIMIT 1"
    stmt = nil
    guard sqlite3_prepare_v2(db, suffSql, -1, &stmt, nil) == SQLITE_OK, let s2 = stmt else { return nil }
    defer { sqlite3_finalize(s2) }
    bindText(s2, 1, "%/\(path)")
    bindText(s2, 2, "%/\(path).%")
    bindText(s2, 3, repoId)
    if sqlite3_step(s2) == SQLITE_ROW, let t = sqlite3_column_text(s2, 0) {
      return String(cString: t)
    }
    return nil
  }

  // MARK: - Query Helper

  private func queryDeps(sql: String, binder: (OpaquePointer) -> Void) throws -> [RAGDependencyResult] {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    var stmt: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
    guard result == SQLITE_OK, let stmt else { throw RAGError.sqlite(String(cString: sqlite3_errmsg(db))) }
    defer { sqlite3_finalize(stmt) }
    binder(stmt)

    var results: [RAGDependencyResult] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
      let sf = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
      let tp = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
      let tf = sqlite3_column_text(stmt, 2).map { String(cString: $0) }
      let dt = sqlite3_column_text(stmt, 3).map { String(cString: $0) } ?? "import"
      let ri = sqlite3_column_text(stmt, 4).map { String(cString: $0) } ?? ""
      results.append(RAGDependencyResult(
        sourceFile: sf, targetPath: tp, targetFile: tf,
        dependencyType: RAGDependencyType(rawValue: dt) ?? .import,
        rawImport: ri
      ))
    }
    return results
  }
}
