//
//  EmbeddingProvider.swift
//  RAGCore
//
//  Protocol for embedding generation, decoupled from any specific ML framework.
//  Consumers inject their own implementation (MLX, CoreML, OpenAI, etc.).
//

import Foundation

/// Protocol for generating vector embeddings from text.
///
/// RAGCore uses this to generate embeddings during indexing and search.
/// Implementations can use any embedding backend (MLX, CoreML, Apple NLEmbedding,
/// remote APIs, or even a hash-based fallback for testing).
///
/// Example conformance:
/// ```swift
/// actor MyMLXProvider: EmbeddingProvider {
///   let dimensions = 768
///   let modelName = "nomic-embed-text-v1.5"
///   func embed(texts: [String]) async throws -> [[Float]] { ... }
/// }
/// ```
public protocol EmbeddingProvider: Sendable {
  /// Generate embedding vectors for the given texts.
  /// - Parameter texts: Array of text strings to embed.
  /// - Returns: Array of float vectors, one per input text.
  func embed(texts: [String]) async throws -> [[Float]]

  /// The dimensionality of the embedding vectors produced.
  var dimensions: Int { get }

  /// A human-readable name for the embedding model (for logging/display).
  var modelName: String { get }
}

/// Optional hook for providers that need cleanup between batches.
/// RAGCore calls `didCompleteBatch()` after each embedding batch during indexing,
/// allowing providers to release GPU memory or other resources.
public protocol BatchAwareEmbeddingProvider: EmbeddingProvider {
  /// Called after each batch of embeddings is generated during indexing.
  /// Use this to clear GPU caches, release buffers, etc.
  func didCompleteBatch() async
}
