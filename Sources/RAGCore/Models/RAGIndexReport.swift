//
//  RAGIndexReport.swift
//  RAGCore
//
//  Report produced after indexing a repository.
//

import Foundation

/// Report produced after indexing a repository or workspace.
public struct RAGIndexReport: Sendable {
  public let repoId: String
  public let repoPath: String
  public let filesIndexed: Int
  public let filesSkipped: Int
  public let chunksIndexed: Int
  public let bytesScanned: Int
  public let durationMs: Int
  public let embeddingCount: Int
  public let embeddingDurationMs: Int

  // AST chunking stats
  public let astFilesChunked: Int
  public let lineFilesChunked: Int
  public let chunkingFailures: Int

  /// Sub-reports for workspace indexing (one per sub-repo/sub-package).
  public let subReports: [RAGIndexReport]

  public init(
    repoId: String, repoPath: String,
    filesIndexed: Int, filesSkipped: Int, chunksIndexed: Int, bytesScanned: Int,
    durationMs: Int, embeddingCount: Int, embeddingDurationMs: Int,
    astFilesChunked: Int, lineFilesChunked: Int, chunkingFailures: Int,
    subReports: [RAGIndexReport] = []
  ) {
    self.repoId = repoId
    self.repoPath = repoPath
    self.filesIndexed = filesIndexed
    self.filesSkipped = filesSkipped
    self.chunksIndexed = chunksIndexed
    self.bytesScanned = bytesScanned
    self.durationMs = durationMs
    self.embeddingCount = embeddingCount
    self.embeddingDurationMs = embeddingDurationMs
    self.astFilesChunked = astFilesChunked
    self.lineFilesChunked = lineFilesChunked
    self.chunkingFailures = chunkingFailures
    self.subReports = subReports
  }
}
