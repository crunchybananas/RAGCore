//
//  RAGStore+Index.swift
//  RAGCore
//
//  Repository indexing pipeline: scan → chunk → embed → store.
//

import CSQLite
import Foundation

extension RAGStore {

  /// Index a repository without progress reporting.
  public func indexRepository(path: String) async throws -> RAGIndexReport {
    try await indexRepository(path: path, forceReindex: false, allowWorkspace: false, excludeSubrepos: true, progress: nil)
  }

  /// Index a repository with full options and progress reporting.
  ///
  /// - Parameters:
  ///   - path: Absolute path to the repository root.
  ///   - forceReindex: If true, re-index all files even if unchanged.
  ///   - allowWorkspace: If true, index as a flat workspace (skip workspace detection).
  ///   - excludeSubrepos: If true, exclude sub-repo directories when workspace scanning.
  ///   - progress: Optional callback for progress updates.
  /// - Returns: An index report with statistics.
  public func indexRepository(
    path: String,
    forceReindex: Bool = false,
    allowWorkspace: Bool = false,
    excludeSubrepos: Bool = true,
    progress: RAGProgressCallback?
  ) async throws -> RAGIndexReport {
    let startTime = Date()
    _ = try initialize()
    logMemory("index start")

    let repoURL = URL(fileURLWithPath: path)
    let workspaceRepos = detectWorkspaceRepos(rootURL: repoURL)

    // Workspace auto-indexing: only split into sub-indexes when multiple
    // separate git repos are detected (e.g., a workspace folder containing
    // several cloned repos). Sub-packages within a single repo (nested
    // package.json, Package.swift, etc.) should NOT cause splitting — those
    // are part of the same codebase and belong in one index.
    if workspaceRepos.count >= 2 && !allowWorkspace {
      let subPackages = detectSubPackages(rootURL: repoURL, excludingGitRepos: workspaceRepos)
      let allSubPaths = workspaceRepos + subPackages
      let parentRepoId = VectorMath.stableId(for: path)
      let parentName = repoURL.lastPathComponent
      let now = dateFormatter.string(from: Date())
      let parentIdentifier = Self.discoverNormalizedRemoteURL(for: path)

      try upsertRepo(id: parentRepoId, name: parentName, rootPath: path, lastIndexedAt: now, repoIdentifier: parentIdentifier, parentRepoId: nil)

      var subReports: [RAGIndexReport] = []
      var totalFiles = 0, totalSkipped = 0, totalRemoved = 0, totalChunks = 0
      var totalBytes = 0, totalEmbeddings = 0, totalEmbeddingMs = 0
      var totalAST = 0, totalLine = 0, totalFailures = 0

      print("[RAG] Workspace detected at \(path): auto-indexing \(allSubPaths.count) sub-packages")
      for (idx, subPath) in allSubPaths.enumerated() {
        let subURL = URL(fileURLWithPath: subPath)
        let subRepoId = VectorMath.stableId(for: subPath)
        let subName = subURL.lastPathComponent
        print("[RAG] Indexing sub-package \(idx + 1)/\(allSubPaths.count): \(subName)")

        let subIdentifier = Self.discoverNormalizedRemoteURL(for: subPath)
        let subNow = dateFormatter.string(from: Date())
        try upsertRepo(id: subRepoId, name: subName, rootPath: subPath, lastIndexedAt: subNow, repoIdentifier: subIdentifier, parentRepoId: parentRepoId)

        let subReport = try await indexRepository(
          path: subPath,
          forceReindex: forceReindex,
          allowWorkspace: true,
          excludeSubrepos: true,
          progress: progress
        )
        subReports.append(subReport)
        totalFiles += subReport.filesIndexed
        totalSkipped += subReport.filesSkipped
        totalRemoved += subReport.filesRemoved
        totalChunks += subReport.chunksIndexed
        totalBytes += subReport.bytesScanned
        totalEmbeddings += subReport.embeddingCount
        totalEmbeddingMs += subReport.embeddingDurationMs
        totalAST += subReport.astFilesChunked
        totalLine += subReport.lineFilesChunked
        totalFailures += subReport.chunkingFailures
      }

      let durationMs = Int(Date().timeIntervalSince(startTime) * 1000)
      let report = RAGIndexReport(
        repoId: parentRepoId,
        repoPath: path,
        filesIndexed: totalFiles,
        filesSkipped: totalSkipped,
        filesRemoved: totalRemoved,
        chunksIndexed: totalChunks,
        bytesScanned: totalBytes,
        durationMs: durationMs,
        embeddingCount: totalEmbeddings,
        embeddingDurationMs: totalEmbeddingMs,
        astFilesChunked: totalAST,
        lineFilesChunked: totalLine,
        chunkingFailures: totalFailures,
        subReports: subReports
      )
      progress?(.complete(report: report))
      return report
    }

    // Single repo — index normally
    let excludedRoots = (allowWorkspace && excludeSubrepos) ? workspaceRepos : []
    let scannedFiles = scanner.scan(rootURL: repoURL, excludingRoots: excludedRoots)
    logMemory("after scan \(scannedFiles.count) files")
    progress?(.scanning(fileCount: scannedFiles.count))

    let repoId = VectorMath.stableId(for: path)
    let repoName = repoURL.lastPathComponent
    let now = dateFormatter.string(from: Date())
    let repoIdentifier = Self.discoverNormalizedRemoteURL(for: path)

    try upsertRepo(id: repoId, name: repoName, rootPath: path, lastIndexedAt: now, repoIdentifier: repoIdentifier)

    var chunkCount = 0
    var bytesScanned = 0
    var embeddingCount = 0
    var embeddingDurationMs = 0
    var skippedUnchanged = 0
    var astFilesChunked = 0
    var lineFilesChunked = 0
    var chunkingFailures = 0

    struct MissingEmbedding {
      let textHash: String
      let text: String
    }

    var filesIndexed = 0
    var seenTextHashes = Set<String>()
    var embeddingCache: [String: [Float]] = [:]
    let embeddingBatchSize = 4
    let memoryCheckInterval = 10

    // Throttle progress callbacks to avoid flooding the main thread
    // For large repos (2000+ files), per-file callbacks can lock up the UI
    let progressInterval = max(1, scannedFiles.count / 100)  // ~100 updates total
    var lastEmbeddingProgressTime = Date.distantPast

    for (fileIndex, candidate) in scannedFiles.enumerated() {
      if fileIndex % progressInterval == 0 {
        progress?(.analyzing(current: fileIndex + 1, total: scannedFiles.count, fileName: URL(fileURLWithPath: candidate.path).lastPathComponent))
      }

      // Memory pressure check
      if fileIndex % memoryCheckInterval == 0 {
        logMemory("analyzing \(fileIndex + 1)/\(scannedFiles.count): \(URL(fileURLWithPath: candidate.path).lastPathComponent)")

        if memoryMonitor.isMemoryPressureHigh() {
          print("[RAG] ⚠️ Memory pressure detected, clearing caches")
          embeddingCache.removeAll()
          seenTextHashes.removeAll()
          await memoryMonitor.clearCaches()
          try await Task.sleep(for: .milliseconds(500))
        }
      }

      guard let file = scanner.loadFile(candidate: candidate) else { continue }

      let relativePath = file.path.hasPrefix(path + "/")
        ? String(file.path.dropFirst(path.count + 1))
        : file.path

      let fileId = VectorMath.stableId(for: "\(repoId):\(relativePath)")
      let fileHash = VectorMath.stableId(for: "\(chunker.chunkingSignature):\(file.text)")

      // Incremental: skip unchanged files
      if !forceReindex {
        let existingHash = try fetchFileHashByPath(repoId: repoId, path: relativePath)
        if let existingHash, existingHash == fileHash {
          skippedUnchanged += 1
          bytesScanned += file.byteCount
          continue
        }
      }

      let chunkResult = chunker.chunkSafe(
        text: file.text,
        language: file.language,
        filePath: relativePath,
        fileHash: fileHash,
        healthTracker: healthTracker
      )

      if chunkResult.usedAST { astFilesChunked += 1 } else { lineFilesChunked += 1 }

      if let failureType = chunkResult.failureType {
        chunkingFailures += 1
        healthTracker.recordFailure(
          filePath: relativePath,
          language: file.language,
          errorType: failureType,
          errorMessage: chunkResult.failureMessage,
          fileHash: fileHash
        )
      }

      let chunks = chunkResult.chunks
      let chunkHashes = chunks.map { VectorMath.stableId(for: $0.text) }

      // Find missing embeddings
      var missingEmbeddings: [MissingEmbedding] = []
      for (index, textHash) in chunkHashes.enumerated() {
        if !seenTextHashes.contains(textHash) {
          let cached = try fetchCachedEmbedding(textHash: textHash)
          if cached == nil {
            missingEmbeddings.append(MissingEmbedding(textHash: textHash, text: chunks[index].text))
          }
          seenTextHashes.insert(textHash)
        }
      }

      if !missingEmbeddings.isEmpty {
        let embedStart = Date()

        for batchStart in stride(from: 0, to: missingEmbeddings.count, by: embeddingBatchSize) {
          let batchEnd = min(batchStart + embeddingBatchSize, missingEmbeddings.count)
          let batchTexts = missingEmbeddings[batchStart..<batchEnd].map(\.text)

          let batchEmbeddings = try await embeddingProvider.embed(texts: batchTexts)
          embeddingCount += batchEmbeddings.count

          for (offset, vector) in batchEmbeddings.enumerated() {
            let missing = missingEmbeddings[batchStart + offset]
            embeddingCache[missing.textHash] = vector
            if !vector.isEmpty {
              try upsertCacheEmbedding(textHash: missing.textHash, vector: vector)
            }
          }

          // Throttle embedding progress — at most once per second
          let now = Date()
          if now.timeIntervalSince(lastEmbeddingProgressTime) >= 1.0 {
            lastEmbeddingProgressTime = now
            progress?(.embedding(current: batchEnd, total: missingEmbeddings.count))
          }

          // Clear caches after each batch to prevent memory accumulation
          if let batchAware = embeddingProvider as? BatchAwareEmbeddingProvider {
            await batchAware.didCompleteBatch()
          }
          await memoryMonitor.clearCaches()
        }

        let embedDuration = Int(Date().timeIntervalSince(embedStart) * 1000)
        embeddingDurationMs += embedDuration
        embeddingCache.removeAll(keepingCapacity: false)
      }

      if filesIndexed % progressInterval == 0 {
        progress?(.storing(current: filesIndexed + 1, total: scannedFiles.count))
      }

      let modulePath = extractModulePath(from: relativePath)
      let featureTags = extractFeatureTags(from: relativePath, language: file.language, chunks: chunks)
      let featureTagsJson = featureTags.isEmpty ? nil : (try? JSONEncoder().encode(featureTags)).flatMap { String(data: $0, encoding: .utf8) }

      let lineCount = chunks.map(\.endLine).max() ?? 0
      let methodCount = chunks.filter { chunk in
        guard let ct = chunk.constructType?.lowercased() else { return false }
        return ct == "function" || ct == "method" || ct == "init" || ct == "deinit"
      }.count

      try upsertFile(
        id: fileId, repoId: repoId, path: relativePath, hash: fileHash,
        language: file.language, updatedAt: now, modulePath: modulePath,
        featureTags: featureTagsJson, lineCount: lineCount,
        methodCount: methodCount, byteSize: file.byteCount
      )
      try cacheAIAnalysis(for: fileId)
      try deleteChunks(for: fileId)
      try deleteDependencies(for: fileId)
      try deleteSymbolRefs(for: fileId)

      for (index, chunk) in chunks.enumerated() {
        let chunkId = VectorMath.stableId(for: "\(fileId):\(chunk.startLine):\(chunk.endLine):\(chunk.text)")
        let textHash = chunkHashes[index]
        let cachedAnalysis = try fetchCachedAIAnalysis(textHash: textHash)
        try upsertChunk(
          id: chunkId, fileId: fileId, startLine: chunk.startLine,
          endLine: chunk.endLine, text: chunk.text, tokenCount: chunk.tokenCount,
          constructType: chunk.constructType, constructName: chunk.constructName,
          metadata: chunk.metadata, aiSummary: cachedAnalysis?.summary,
          aiTags: cachedAnalysis?.tags,
          analyzedAt: cachedAnalysis != nil ? dateFormatter.string(from: Date()) : nil,
          analyzerModel: cachedAnalysis?.model
        )

        let embedding: [Float]
        if let cached = embeddingCache[textHash] {
          embedding = cached
        } else if let dbCached = try fetchCachedEmbedding(textHash: textHash) {
          embedding = dbCached
        } else {
          embedding = []
        }

        if !embedding.isEmpty {
          try upsertEmbedding(chunkId: chunkId, vector: embedding)
        }
      }

      // Dependencies and symbol refs
      let fileDeps = extractDependencies(
        from: chunks, repoId: repoId, fileId: fileId,
        relativePath: relativePath, language: file.language
      )
      if !fileDeps.isEmpty { try insertDependencies(fileDeps) }

      let symbolRefs = extractSymbolRefs(
        from: chunks, repoId: repoId, fileId: fileId
      )
      if !symbolRefs.isEmpty { try insertSymbolRefs(symbolRefs) }

      chunkCount += chunks.count
      bytesScanned += file.byteCount
      filesIndexed += 1
    }

    logMemory("index complete")
    print("[RAG] AST stats: \(astFilesChunked) AST, \(lineFilesChunked) line-based, \(chunkingFailures) failures")

    // Prune files from the index that no longer exist on disk
    let currentPaths = Set(scannedFiles.map { file -> String in
      let filePath = file.path
      return filePath.hasPrefix(path + "/")
        ? String(filePath.dropFirst(path.count + 1))
        : filePath
    })
    let filesRemovedCount = try pruneDeletedFiles(repoId: repoId, currentPaths: currentPaths)
    if filesRemovedCount > 0 {
      print("[RAG] Pruned \(filesRemovedCount) deleted files from index")
    }

    let durationMs = Int(Date().timeIntervalSince(startTime) * 1000)
    let report = RAGIndexReport(
      repoId: repoId,
      repoPath: path,
      filesIndexed: filesIndexed,
      filesSkipped: skippedUnchanged,
      filesRemoved: filesRemovedCount,
      chunksIndexed: chunkCount,
      bytesScanned: bytesScanned,
      durationMs: durationMs,
      embeddingCount: embeddingCount,
      embeddingDurationMs: embeddingDurationMs,
      astFilesChunked: astFilesChunked,
      lineFilesChunked: lineFilesChunked,
      chunkingFailures: chunkingFailures
    )
    progress?(.complete(report: report))
    return report
  }

  // MARK: - Stale File Pruning

  /// Remove files from the index that no longer exist on disk.
  /// Cascades to chunks → embeddings via FK constraints; vec_chunks
  /// must be cleaned explicitly since virtual tables don't support CASCADE.
  internal func pruneDeletedFiles(repoId: String, currentPaths: Set<String>) throws -> Int {
    guard let db else {
      throw RAGError.sqlite("Database not initialized")
    }

    // Fetch all indexed paths for this repo
    let sql = "SELECT id, path FROM files WHERE repo_id = ?"
    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let statement else {
      let message = String(cString: sqlite3_errmsg(db))
      throw RAGError.sqlite(message)
    }
    defer { sqlite3_finalize(statement) }
    bindText(statement, 1, repoId)

    var staleFiles: [(id: String, path: String)] = []
    while sqlite3_step(statement) == SQLITE_ROW {
      guard let idPtr = sqlite3_column_text(statement, 0),
            let pathPtr = sqlite3_column_text(statement, 1) else { continue }
      let fileId = String(cString: idPtr)
      let filePath = String(cString: pathPtr)
      if !currentPaths.contains(filePath) {
        staleFiles.append((id: fileId, path: filePath))
      }
    }

    guard !staleFiles.isEmpty else { return 0 }

    for staleFile in staleFiles {
      // vec_chunks first (virtual table — no CASCADE)
      if extensionLoaded {
        let vecSql = "DELETE FROM vec_chunks WHERE chunk_id IN (SELECT id FROM chunks WHERE file_id = ?)"
        try execute(sql: vecSql) { stmt in
          bindText(stmt, 1, staleFile.id)
        }
      }
      // Delete dependencies and symbol refs
      try deleteDependencies(for: staleFile.id)
      try deleteSymbolRefs(for: staleFile.id)
      // Delete the file row — chunks and embeddings cascade
      let delSql = "DELETE FROM files WHERE id = ?"
      try execute(sql: delSql) { stmt in
        bindText(stmt, 1, staleFile.id)
      }
    }

    return staleFiles.count
  }
}
