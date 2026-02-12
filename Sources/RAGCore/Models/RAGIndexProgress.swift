//
//  RAGIndexProgress.swift
//  RAGCore
//
//  Progress updates during indexing operations.
//

import Foundation

/// Progress updates emitted during repository indexing.
public enum RAGIndexProgress: Sendable {
  case scanning(fileCount: Int)
  case analyzing(current: Int, total: Int, fileName: String)
  case embedding(current: Int, total: Int)
  case storing(current: Int, total: Int)
  case complete(report: RAGIndexReport)

  public var description: String {
    switch self {
    case .scanning(let count):
      return "Scanning files... (\(count) found)"
    case .analyzing(let current, let total, let fileName):
      return "Analyzing \(current)/\(total): \(fileName)"
    case .embedding(let current, let total):
      return "Generating embeddings... \(current)/\(total)"
    case .storing(let current, let total):
      return "Storing chunks... \(current)/\(total)"
    case .complete(let report):
      return "Complete: \(report.filesIndexed) files, \(report.chunksIndexed) chunks in \(report.durationMs)ms"
    }
  }

  public var progress: Double {
    switch self {
    case .scanning: return 0.1
    case .analyzing(let current, let total, _): return 0.1 + 0.3 * Double(current) / Double(max(1, total))
    case .embedding(let current, let total): return 0.4 + 0.4 * Double(current) / Double(max(1, total))
    case .storing(let current, let total): return 0.8 + 0.2 * Double(current) / Double(max(1, total))
    case .complete: return 1.0
    }
  }

  public var isComplete: Bool {
    if case .complete = self { return true }
    return false
  }
}

/// Callback type for progress updates during indexing.
public typealias RAGProgressCallback = @Sendable (RAGIndexProgress) -> Void
