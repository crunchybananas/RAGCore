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

  /// Analyze a code chunk with the full context the store knows about it,
  /// including where the chunk lives. The store always calls this overload.
  ///
  /// The default forwards to the context-free overload, so an existing
  /// analyzer keeps working unchanged. Implement it to use the file path:
  /// without it a summary cannot say which feature or screen the code belongs
  /// to, and a model left to guess fills that gap with invention.
  func analyze(chunk: String, context: ChunkAnalysisContext) async throws -> ChunkAnalysis

  /// A human-readable name for the analyzer model (for logging/display).
  var analyzerName: String { get }
}

extension ChunkAnalyzer {
  public func analyze(chunk: String, context: ChunkAnalysisContext) async throws -> ChunkAnalysis {
    try await analyze(
      chunk: chunk,
      constructType: context.constructType,
      constructName: context.constructName,
      language: context.language
    )
  }
}

/// Everything the store knows about a chunk besides its text.
public struct ChunkAnalysisContext: Sendable, Equatable {
  /// The file's path relative to its repository root, e.g. "Sources/App/Auth/Login.swift".
  public let filePath: String?
  /// The AST construct type (e.g., "classDecl", "function", "imports"), if known.
  public let constructType: String?
  /// The construct's name (e.g., "UserService", "Cart (part 2/3)"), if known.
  public let constructName: String?
  /// The language label the scanner assigned (e.g., "Swift", "YAML"), if known.
  public let language: String?

  public init(filePath: String?, constructType: String?, constructName: String?, language: String?) {
    self.filePath = filePath
    self.constructType = constructType
    self.constructName = constructName
    self.language = language
  }
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
