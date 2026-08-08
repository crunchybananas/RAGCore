//
//  RAGStore+Index.swift
//  RAGCore
//
//  Repository indexing pipeline: scan → chunk → embed → store.
//

import CSQLite
import Foundation

extension RAGStore {

  /// Sub-repo roots a flat-workspace scan must skip.
  ///
  /// `excludeSubrepos: false` asks for a flat workspace index, which is a
  /// legitimate request only for sub-repos nobody indexes separately. Walking a
  /// checkout that ALREADY has its own row duplicates it outright: same bytes,
  /// same embeddings, two rows, and every search hit inside it returned twice.
  /// Observed on a superproject with 13 submodules — 5,479 of 7,732 files had a
  /// byte-identical twin, and the parent's 21,551 chunks were almost entirely
  /// redundant with the children's.
  ///
  /// So the flag governs UNTRACKED sub-repos only. A tracked one is always
  /// excluded, which makes the duplicate structurally unreachable rather than
  /// contingent on every caller passing the right flag.
  ///
  /// The tracked filter applies when `allowWorkspace` is false too, which closes
  /// a second, quieter case: the workspace split only fires at two or more
  /// sub-repos, so a parent holding exactly ONE tracked child fell through to a
  /// normal scan and absorbed it with no flag involved.
  internal static func excludedSubrepoRoots(
    workspaceRepos: [String],
    allowWorkspace: Bool,
    excludeSubrepos: Bool,
    isTracked: (String) -> Bool
  ) -> [String] {
    if allowWorkspace && excludeSubrepos { return workspaceRepos }
    return workspaceRepos.filter(isTracked)
  }

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
    excludedDirectories: Set<String>? = nil,
    progress: RAGProgressCallback?
  ) async throws -> RAGIndexReport {
    let startTime = Date()
    try Task.checkCancellation()
    _ = try initialize()
    try Task.checkCancellation()
    logMemory("index start")

    let repoURL = URL(fileURLWithPath: path)
    let workspaceRepos = try detectWorkspaceReposCancellable(rootURL: repoURL)
    try Task.checkCancellation()

    // Workspace auto-indexing: only split into sub-indexes when multiple
    // separate git repos are detected (e.g., a workspace folder containing
    // several cloned repos). Sub-packages within a single repo (nested
    // package.json, Package.swift, etc.) should NOT cause splitting — those
    // are part of the same codebase and belong in one index.
    if workspaceRepos.count >= 2 && !allowWorkspace {
      let subPackages = try detectSubPackagesCancellable(rootURL: repoURL, excludingGitRepos: workspaceRepos)
      let allSubPaths = workspaceRepos + subPackages
      let parentRepoId: String
      if let resolved = try resolveRepo(for: path, remapRootPathOnMismatch: true) {
        parentRepoId = resolved.id
      } else {
        parentRepoId = VectorMath.stableId(for: path)
      }
      let parentName = repoURL.lastPathComponent
      let now = dateFormatter.string(from: Date())
      let parentIdentifier = Self.discoverNormalizedRemoteURL(for: path)

      try upsertRepo(
        id: parentRepoId,
        name: parentName,
        rootPath: path,
        lastIndexedAt: now,
        repoIdentifier: parentIdentifier,
        parentRepoId: nil,
        embeddingModel: embeddingProvider.modelName,
        embeddingDimensions: embeddingProvider.dimensions
      )

      var subReports: [RAGIndexReport] = []
      var totalFiles = 0, totalSkipped = 0, totalRemoved = 0, totalChunks = 0
      var totalBytes = 0, totalEmbeddings = 0, totalEmbeddingMs = 0
      var totalAST = 0, totalLine = 0, totalFailures = 0

      print("[RAG] Workspace detected at \(path): auto-indexing \(allSubPaths.count) sub-packages")
      for (idx, subPath) in allSubPaths.enumerated() {
        try Task.checkCancellation()
        let subURL = URL(fileURLWithPath: subPath)
        let subRepoId: String
        if let resolved = try resolveRepo(for: subPath, remapRootPathOnMismatch: true) {
          subRepoId = resolved.id
        } else {
          subRepoId = VectorMath.stableId(for: subPath)
        }
        let subName = subURL.lastPathComponent
        print("[RAG] Indexing sub-package \(idx + 1)/\(allSubPaths.count): \(subName)")

        let subIdentifier = Self.discoverNormalizedRemoteURL(for: subPath)
        let subNow = dateFormatter.string(from: Date())
        try upsertRepo(
          id: subRepoId,
          name: subName,
          rootPath: subPath,
          lastIndexedAt: subNow,
          repoIdentifier: subIdentifier,
          parentRepoId: parentRepoId,
          embeddingModel: embeddingProvider.modelName,
          embeddingDimensions: embeddingProvider.dimensions
        )

        let subReport = try await indexRepository(
          path: subPath,
          forceReindex: forceReindex,
          allowWorkspace: true,
          excludeSubrepos: true,
          progress: progress
        )
        try Task.checkCancellation()
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
      try Task.checkCancellation()
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
    let excludedRoots = Self.excludedSubrepoRoots(
      workspaceRepos: workspaceRepos,
      allowWorkspace: allowWorkspace,
      excludeSubrepos: excludeSubrepos,
      // remap stays off: this asks "does this path already have a row?", it must
      // not rebind anything as a side effect (crunchybananas/RAGCore#2).
      isTracked: { ((try? resolveRepo(for: $0)) ?? nil) != nil }
    )
    let effectiveScanner = excludedDirectories.map { RAGFileScanner(excludedDirectories: $0) } ?? scanner
    let scannedFiles = try effectiveScanner.scanCancellable(rootURL: repoURL, excludingRoots: excludedRoots)
    try Task.checkCancellation()
    logMemory("after scan \(scannedFiles.count) files")
    progress?(.scanning(fileCount: scannedFiles.count))

    let repoId: String
    if let resolved = try resolveRepo(for: path, remapRootPathOnMismatch: true) {
      repoId = resolved.id
    } else {
      repoId = VectorMath.stableId(for: path)
    }
    let repoName = repoURL.lastPathComponent
    let now = dateFormatter.string(from: Date())
    let repoIdentifier = Self.discoverNormalizedRemoteURL(for: path)

    try upsertRepo(
      id: repoId,
      name: repoName,
      rootPath: path,
      lastIndexedAt: now,
      repoIdentifier: repoIdentifier,
      embeddingModel: embeddingProvider.modelName,
      embeddingDimensions: embeddingProvider.dimensions
    )

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

    // Per-run latch: once the embedding provider has failed once with a
    // transport-style error (Ollama offline, server crashed, network
    // partition), stop attempting embeddings for the rest of the run.
    // Files still get chunked, indexed, and symbol-graph-extracted —
    // they're searchable via TEXT mode and via rag.references /
    // rag.definitions immediately. Vector embeddings can be backfilled
    // later by re-running rag.index once the provider is reachable
    // (incremental indexing re-checks unembedded chunks). This makes
    // indexing resilient to a flaky local Ollama without spamming logs
    // with N copies of the same connection error.
    var embeddingDisabledForRun = false
    var skippedEmbeddingFiles = 0

    // Throttle progress callbacks to avoid flooding the main thread
    // For large repos (2000+ files), per-file callbacks can lock up the UI
    let progressInterval = max(1, scannedFiles.count / 100)  // ~100 updates total
    var lastEmbeddingProgressTime = Date.distantPast

    for (fileIndex, candidate) in scannedFiles.enumerated() {
      try Task.checkCancellation()
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
          try Task.checkCancellation()
        }
      }

      guard let file = effectiveScanner.loadFile(candidate: candidate) else { continue }
      try Task.checkCancellation()

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
      try Task.checkCancellation()

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
      } else if chunkResult.usedAST {
        // AST chunking succeeded — clear any stale failure record so a
        // one-off timeout doesn't keep us on the line-chunked path forever.
        healthTracker.recordSuccess(filePath: relativePath)
      }

      let chunks = chunkResult.chunks
      let chunkHashes = chunks.map { VectorMath.stableId(for: $0.text) }

      // Find missing embeddings
      var missingEmbeddings: [MissingEmbedding] = []
      for (index, textHash) in chunkHashes.enumerated() {
        try Task.checkCancellation()
        if !seenTextHashes.contains(textHash) {
          let cached = try fetchCachedEmbedding(textHash: textHash)
          if cached == nil {
            missingEmbeddings.append(MissingEmbedding(textHash: textHash, text: chunks[index].text))
          }
          seenTextHashes.insert(textHash)
        }
      }

      if !missingEmbeddings.isEmpty && !embeddingDisabledForRun {
        let embedStart = Date()
        var aborted = false

        batchLoop: for batchStart in stride(from: 0, to: missingEmbeddings.count, by: embeddingBatchSize) {
          try Task.checkCancellation()
          let batchEnd = min(batchStart + embeddingBatchSize, missingEmbeddings.count)
          let batchTexts = missingEmbeddings[batchStart..<batchEnd].map(\.text)

          let batchEmbeddings: [[Float]]
          do {
            batchEmbeddings = try await embeddingProvider.embed(texts: batchTexts)
          } catch {
            if error is CancellationError || Task.isCancelled {
              throw CancellationError()
            }
            // Likely transport (Ollama offline) or rate-limit. Latch
            // embeddings off for the rest of the run, log once, and let
            // the caller see chunks/symbols still get persisted. The
            // alternative — propagating — turns a transient embedder
            // outage into "every file in the repo failed to index",
            // which is what previously made `rag.index` unusable when
            // Ollama wasn't reachable.
            print("[RAG] Embedding disabled for the rest of this run: \(error.localizedDescription). Files will still be chunked + indexed; rerun rag.index once the embedder is reachable to backfill vectors.")
            embeddingDisabledForRun = true
            skippedEmbeddingFiles += 1
            aborted = true
            break batchLoop
          }
          try Task.checkCancellation()
          embeddingCount += batchEmbeddings.count

          for (offset, vector) in batchEmbeddings.enumerated() {
            try Task.checkCancellation()
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
            try Task.checkCancellation()
          }
          await memoryMonitor.clearCaches()
          try Task.checkCancellation()
        }

        let embedDuration = Int(Date().timeIntervalSince(embedStart) * 1000)
        embeddingDurationMs += embedDuration
        embeddingCache.removeAll(keepingCapacity: false)
        // If we aborted partway through, the embeddingCache holds what
        // succeeded before the failure — those chunks will store with
        // vectors. Subsequent files in this run skip embedding entirely.
        _ = aborted
      } else if !missingEmbeddings.isEmpty {
        // Embeddings already disabled for the run (an earlier file's
        // embed threw). Track the count so the report can surface how
        // many files we skipped vector embedding for.
        skippedEmbeddingFiles += 1
      }

      if filesIndexed % progressInterval == 0 {
        progress?(.storing(current: filesIndexed + 1, total: scannedFiles.count))
      }
      try Task.checkCancellation()

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
      try deleteSymbols(for: fileId)

      for (index, chunk) in chunks.enumerated() {
        try Task.checkCancellation()
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
      try Task.checkCancellation()
      let fileDeps = extractDependencies(
        from: chunks, repoId: repoId, fileId: fileId,
        relativePath: relativePath, language: file.language
      )
      if !fileDeps.isEmpty { try insertDependencies(fileDeps) }

      let symbolRefs = extractSymbolRefs(
        from: chunks, repoId: repoId, fileId: fileId
      )
      if !symbolRefs.isEmpty { try insertSymbolRefs(symbolRefs) }

      let symbolDefs = extractSymbolDefinitions(
        from: chunks, repoId: repoId, fileId: fileId
      )
      if !symbolDefs.isEmpty { try insertSymbols(symbolDefs) }

      chunkCount += chunks.count
      bytesScanned += file.byteCount
      filesIndexed += 1
    }

    logMemory("index complete")
    print("[RAG] AST stats: \(astFilesChunked) AST, \(lineFilesChunked) line-based, \(chunkingFailures) failures")

    // Prune files from the index that no longer exist on disk
    try Task.checkCancellation()
    let currentPaths = Set(scannedFiles.map { file -> String in
      let filePath = file.path
      return filePath.hasPrefix(path + "/")
        ? String(filePath.dropFirst(path.count + 1))
        : filePath
    })
    let filesRemovedCount = try pruneDeletedFiles(repoId: repoId, currentPaths: currentPaths)
    try Task.checkCancellation()
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
      chunkingFailures: chunkingFailures,
      embeddingSkippedFiles: skippedEmbeddingFiles
    )
    progress?(.complete(report: report))
    return report
  }

  // MARK: - Stale File Pruning

  /// Remove files from the index that no longer exist on disk.
  ///
  /// Every child table is deleted EXPLICITLY, in child-before-parent order.
  /// Nothing here cascades: the schema declares plain
  /// `FOREIGN KEY (file_id) REFERENCES files(id)` with no `ON DELETE CASCADE`,
  /// and SQLite's `foreign_keys` pragma defaults to OFF and is never turned on
  /// anywhere in RAGCore, so a bare `DELETE FROM files` abandons its rows
  /// rather than cascading to them.
  ///
  /// This function used to delete `dependencies`, `symbol_refs` and `symbols`
  /// by hand while assuming `chunks` and `embeddings` cascaded. They did not.
  /// Because pruning runs on every index refresh, each renamed or deleted file
  /// leaked its chunks and embeddings, and the orphans accumulated for as long
  /// as the index has existed (cloke/peel#1881: 11,218 dangling chunks and
  /// 10,906 dangling embeddings on one machine, still growing when measured).
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
      try Task.checkCancellation()
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
      try Task.checkCancellation()
      // One function owns "remove a file's chunks and everything keyed on
      // them" — vec_chunks, embeddings, then chunks. Duplicating that list is
      // exactly how embeddings came to be missing from one copy of it.
      try deleteChunks(for: staleFile.id)
      try deleteDependencies(for: staleFile.id)
      try deleteSymbolRefs(for: staleFile.id)
      try deleteSymbols(for: staleFile.id)
      // Finally the parent row, now that nothing references it.
      let delSql = "DELETE FROM files WHERE id = ?"
      try execute(sql: delSql) { stmt in
        bindText(stmt, 1, staleFile.id)
      }
    }

    return staleFiles.count
  }
}
