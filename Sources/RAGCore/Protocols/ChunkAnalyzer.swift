//
//  ChunkAnalyzer.swift
//  RAGCore
//
//  Protocol for AI-powered chunk analysis, decoupled from any specific ML framework.
//  Consumers inject their own implementation (MLX chat model, OpenAI, etc.).
//

import Foundation

/// Protocol for AI-powered analysis of code chunks.
///
/// RAGCore optionally uses this to enrich indexed chunks with AI-generated
/// summaries and semantic tags. When no analyzer is provided, chunks are
/// indexed without AI analysis.
///
/// Example conformance:
/// ```swift
/// actor MyMLXAnalyzer: ChunkAnalyzer {
///   let analyzerName = "Qwen3-1.7B"
///   func analyze(chunk: String, constructType: String?, ...) async throws -> ChunkAnalysis { ... }
/// }
/// ```
public protocol ChunkAnalyzer: Sendable {
  /// Analyze a code chunk and produce a summary and semantic tags.
  ///
  /// - Parameters:
  ///   - chunk: The source code text of the chunk.
  ///   - constructType: The AST construct type (e.g., "class", "function"), if known.
  ///   - constructName: The name of the construct (e.g., "UserService"), if known.
  ///   - language: The programming language (e.g., "Swift", "TypeScript"), if known.
  /// - Returns: A `ChunkAnalysis` with summary and tags.
  func analyze(
    chunk: String,
    constructType: String?,
    constructName: String?,
    language: String?
  ) async throws -> ChunkAnalysis

  /// A human-readable name for the analyzer model (for logging/display).
  var analyzerName: String { get }
}

/// Result of AI analysis on a code chunk.
public struct ChunkAnalysis: Sendable {
  /// A concise summary of what this chunk does.
  public let summary: String

  /// Semantic tags describing the chunk's purpose (e.g., ["error-handling", "validation"]).
  public let tags: [String]

  public init(summary: String, tags: [String]) {
    self.summary = summary
    self.tags = tags
  }
}
