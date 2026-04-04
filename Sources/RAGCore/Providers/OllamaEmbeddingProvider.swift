//
//  OllamaEmbeddingProvider.swift
//  RAGCore
//
//  Embedding provider backed by Ollama's /api/embed endpoint.
//  Uses Ollama's native MLX backend on Apple Silicon for fast,
//  out-of-process embedding generation with automatic model lifecycle.
//

import Foundation

/// Embedding provider that delegates to Ollama's /api/embed endpoint.
/// Ollama manages the model lifecycle (loading, unloading, GPU memory)
/// in its own process — no in-app Metal management needed.
public final class OllamaEmbeddingProvider: EmbeddingProvider, BatchAwareEmbeddingProvider, @unchecked Sendable {
  public let modelName: String
  public private(set) var dimensions: Int
  private let baseURL: String

  /// Known embedding dimensions for common Ollama models.
  /// Avoids an async probe at init time (RAGCore reads dimensions synchronously).
  public static let knownDimensions: [String: Int] = [
    "qwen3-embedding:8b": 4096,
    "qwen3-embedding:4b": 2048,
    "qwen3-embedding:0.6b": 1024,
    "qwen3-embedding": 4096,
    "nomic-embed-text": 768,
    "nomic-embed-text:latest": 768,
    "mxbai-embed-large": 1024,
    "mxbai-embed-large:latest": 1024,
    "all-minilm": 384,
    "all-minilm:latest": 384,
    "snowflake-arctic-embed": 1024,
    "snowflake-arctic-embed:latest": 1024,
  ]

  /// Create a provider for a specific Ollama embedding model.
  /// - Parameters:
  ///   - model: Ollama model name (e.g. "nomic-embed-text", "qwen3-embedding:8b")
  ///   - dimensions: Override dimensions (0 = auto-detect from known models or probe on first call)
  ///   - baseURL: Ollama API base URL (default: http://localhost:11434)
  public init(model: String, dimensions: Int = 0, baseURL: String = "http://localhost:11434") {
    self.modelName = model
    self.dimensions = dimensions > 0 ? dimensions : (Self.knownDimensions[model] ?? 0)
    self.baseURL = baseURL
  }

  public func embed(texts: [String]) async throws -> [[Float]] {
    guard !texts.isEmpty else { return [] }

    let result = try await callOllamaEmbed(texts: texts, model: modelName)

    // Capture dimensions on first successful call
    if dimensions == 0, let first = result.first {
      dimensions = first.count
    }

    return result
  }

  public func didCompleteBatch() async {
    // No-op — Ollama manages model lifecycle via its own keep-alive timers.
  }

  private func callOllamaEmbed(texts: [String], model: String) async throws -> [[Float]] {
    let url = URL(string: "\(baseURL)/api/embed")!
    var request = URLRequest(url: url)
    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.timeoutInterval = 120

    // Truncate long texts to stay within model context length.
    let isLargeContext = model.contains("qwen3-embedding")
    let maxChars = isLargeContext ? 8000 : 2000
    let truncated = texts.map { $0.count > maxChars ? String($0.prefix(maxChars)) : $0 }
    let numCtx = isLargeContext ? 32768 : 8192
    let body: [String: Any] = ["model": model, "input": truncated, "options": ["num_ctx": numCtx]]
    request.httpBody = try JSONSerialization.data(withJSONObject: body)

    let (data, response) = try await URLSession.shared.data(for: request)

    guard let http = response as? HTTPURLResponse, (200...299).contains(http.statusCode) else {
      let code = (response as? HTTPURLResponse)?.statusCode ?? -1
      let body = String(data: data, encoding: .utf8) ?? "<no body>"
      throw OllamaError.httpError(code: code, body: body)
    }

    guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          let embeddings = json["embeddings"] as? [[Double]] else {
      throw OllamaError.invalidResponse("Missing embeddings in response")
    }

    return embeddings.map { $0.map(Float.init) }
  }
}
