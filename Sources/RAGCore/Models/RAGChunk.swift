//
//  RAGChunk.swift
//  RAGCore
//
//  Chunk types for code splitting and processing.
//

import Foundation

/// A chunk of source code extracted by the chunking pipeline.
public struct RAGChunk: Sendable {
  public let startLine: Int
  public let endLine: Int
  public let text: String
  public let tokenCount: Int

  // AST metadata (nil for line-based chunks)
  public let constructType: String?
  public let constructName: String?

  /// JSON-encoded metadata from AST analysis (decorators, protocols, imports, etc.)
  public let metadata: String?

  public init(
    startLine: Int,
    endLine: Int,
    text: String,
    tokenCount: Int,
    constructType: String? = nil,
    constructName: String? = nil,
    metadata: String? = nil
  ) {
    self.startLine = startLine
    self.endLine = endLine
    self.text = text
    self.tokenCount = tokenCount
    self.constructType = constructType
    self.constructName = constructName
    self.metadata = metadata
  }
}

/// A file candidate discovered during scanning.
public struct RAGFileCandidate: Sendable {
  public let path: String
  public let byteCount: Int
  public let language: String

  public init(path: String, byteCount: Int, language: String) {
    self.path = path
    self.byteCount = byteCount
    self.language = language
  }
}

/// A scanned file loaded into memory for chunking.
public struct RAGScannedFile: Sendable {
  public let path: String
  public let text: String
  public let lineCount: Int
  public let byteCount: Int
  public let language: String

  public init(path: String, text: String, lineCount: Int, byteCount: Int, language: String) {
    self.path = path
    self.text = text
    self.lineCount = lineCount
    self.byteCount = byteCount
    self.language = language
  }
}

/// Result of chunking a file, with metadata about how it was processed.
public struct ChunkingResult: Sendable {
  public let chunks: [RAGChunk]
  public let usedAST: Bool
  public let failureType: ChunkingHealthTracker.FailureType?
  public let failureMessage: String?

  public init(
    chunks: [RAGChunk],
    usedAST: Bool,
    failureType: ChunkingHealthTracker.FailureType? = nil,
    failureMessage: String? = nil
  ) {
    self.chunks = chunks
    self.usedAST = usedAST
    self.failureType = failureType
    self.failureMessage = failureMessage
  }
}
