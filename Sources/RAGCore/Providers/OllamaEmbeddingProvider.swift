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
  /// A conservative provider-level cap shared by indexing, enrichment, search,
  /// and direct callers. RAGStore also batches, but the provider is the only
  /// boundary that sees every request shape.
  static let maxEmbedRequestInputBytes = 16_000
  static let maxEmbedRequestCount = 4

  public let modelName: String
  public private(set) var dimensions: Int
  private let baseURL: String
  private let requestGate: OllamaEmbeddingRequestGate

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
    self.requestGate = .shared
  }

  init(
    model: String,
    dimensions: Int = 0,
    baseURL: String = "http://localhost:11434",
    requestGate: OllamaEmbeddingRequestGate
  ) {
    self.modelName = model
    self.dimensions = dimensions > 0 ? dimensions : (Self.knownDimensions[model] ?? 0)
    self.baseURL = baseURL
    self.requestGate = requestGate
  }

  public func embed(texts: [String]) async throws -> [[Float]] {
    guard !texts.isEmpty else { return [] }

    let prepared = Self.prepare(texts: texts, model: modelName)
    var result: [[Float]] = []
    result.reserveCapacity(prepared.count)
    for range in Self.requestRanges(for: prepared) {
      let batch = Array(prepared[range])
      let batchResult = try await requestGate.perform(
        endpoint: baseURL,
        model: modelName
      ) { [baseURL, modelName] in
        try await Self.callOllamaEmbed(
          texts: batch,
          model: modelName,
          baseURL: baseURL
        )
      }
      guard batchResult.count == batch.count else {
        throw OllamaError.invalidResponse(
          "Expected \(batch.count) embeddings, received \(batchResult.count)"
        )
      }
      result.append(contentsOf: batchResult)
    }

    // Capture dimensions on first successful call
    if dimensions == 0, let first = result.first {
      dimensions = first.count
    }

    return result
  }

  public func didCompleteBatch() async {
    // No-op — Ollama manages model lifecycle via its own keep-alive timers.
  }

  static func requestRanges(for texts: [String]) -> [Range<Int>] {
    var ranges: [Range<Int>] = []
    var start = 0
    var bytes = 0
    for index in texts.indices {
      let size = texts[index].utf8.count
      let wouldExceedBytes = bytes > 0
        && bytes + size > maxEmbedRequestInputBytes
      let wouldExceedCount = index - start >= maxEmbedRequestCount
      if wouldExceedBytes || wouldExceedCount {
        ranges.append(start..<index)
        start = index
        bytes = 0
      }
      bytes += size
    }
    if start < texts.count {
      ranges.append(start..<texts.count)
    }
    return ranges
  }

  static func prepare(texts: [String], model: String) -> [String] {
    let maxChars = model.contains("qwen3-embedding") ? 8_000 : 2_000
    return texts.map { text in
      text.count > maxChars ? String(text.prefix(maxChars)) : text
    }
  }

  private static func callOllamaEmbed(
    texts: [String],
    model: String,
    baseURL: String
  ) async throws -> [[Float]] {
    guard let url = URL(string: "\(baseURL)/api/embed") else {
      throw URLError(.badURL)
    }
    var request = URLRequest(url: url)
    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.timeoutInterval = 120

    let numCtx = model.contains("qwen3-embedding") ? 32768 : 8192
    let body: [String: Any] = ["model": model, "input": texts, "options": ["num_ctx": numCtx]]
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
