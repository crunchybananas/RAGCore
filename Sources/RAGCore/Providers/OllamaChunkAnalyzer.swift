//
//  OllamaChunkAnalyzer.swift
//  RAGCore
//
//  Chunk analyzer that uses Ollama models (Gemma 4, Qwen3, etc.) for
//  semantic analysis of code chunks. Out-of-process — no in-app model
//  loading or GPU management needed.
//

import Foundation

/// Code chunk analyzer backed by Ollama's /api/chat endpoint.
/// Uses whatever model you specify — Gemma 4, Qwen3, etc.
public actor OllamaChunkAnalyzer: ChunkAnalyzer {
  public let analyzerName: String
  private let model: String
  private let baseURL: String
  private let requestTimeout: TimeInterval
  private let keepAlive: String

  /// Create an analyzer for a specific Ollama model.
  /// - Parameters:
  ///   - model: Ollama model name (e.g. "gemma3:12b", "qwen3:8b")
  ///   - baseURL: Ollama API base URL (default: http://localhost:11434)
  ///   - requestTimeout: client-side deadline per chat request. The default
  ///     is sized for a COLD model load, not a warm inference: Ollama's
  ///     5-minute default keep-alive evicts any analyzer on a machine that
  ///     runs more than one model, and a 17-30 GB model takes minutes to page
  ///     back in. The old 60s deadline made the first request after every
  ///     eviction a guaranteed failure — 1 chunk analyzed, 16 failed in 21
  ///     minutes, measured 2026-08-14 (#22).
  ///   - keepAlive: Ollama `keep_alive` sent with every request, so a long
  ///     analysis run holds its own model resident instead of depending on
  ///     the server default that evicted it in the first place.
  public init(
    model: String,
    baseURL: String = "http://localhost:11434",
    requestTimeout: TimeInterval = 600,
    keepAlive: String = "30m"
  ) {
    self.model = model
    self.analyzerName = model
    self.baseURL = baseURL
    self.requestTimeout = max(1, requestTimeout)
    self.keepAlive = keepAlive
  }

  /// Whether this model wants the `/no_think` thinking-suppression prefix.
  ///
  /// Matches the model IDENTITY — the final path component — not the raw
  /// tag: namespaced pulls (`hf.co/unsloth/Qwen3-…`, `moophlo/Qwen3-Coder-…`)
  /// prefix the source, so a bare `hasPrefix("qwen3")` on the whole tag
  /// missed exactly the models the guard exists for and paid the verbose
  /// thinking tax (~376 tokens/6s vs ~75 tokens/1.5s per chunk — #23).
  /// Embedding models are excluded as before. `/no_think` is a no-op on Qwen
  /// lines whose reasoning is an API-level control (measured on Qwen3.8) —
  /// harmless there, effective where it is a prompt token.
  static func wantsNoThinkPrefix(model: String) -> Bool {
    let lowered = model.lowercased()
    guard !lowered.contains("embed") else { return false }
    let identity = lowered.split(separator: "/").last.map(String.init) ?? lowered
    return identity.hasPrefix("qwen3")
  }

  public func analyze(
    chunk: String,
    constructType: String?,
    constructName: String?,
    language: String?
  ) async throws -> ChunkAnalysis {
    try await analyze(
      chunk: chunk,
      context: ChunkAnalysisContext(
        filePath: nil, constructType: constructType, constructName: constructName, language: language
      )
    )
  }

  public func analyze(chunk: String, context: ChunkAnalysisContext) async throws -> ChunkAnalysis {
    let prompt = ChunkAnalysisPrompt.userMessage(chunk: chunk, context: context)
    // Qwen3 extended thinking mode produces verbose output, so disable it.
    let userContent = Self.wantsNoThinkPrefix(model: model) ? "/no_think\n\(prompt)" : prompt

    let content: String
    do {
      content = try await chat(userContent: userContent, format: structuredOutputRejected ? "json" : ChunkAnalysisPrompt.responseSchema)
    } catch OllamaError.httpError(let code, _) where code == 400 && !structuredOutputRejected {
      // An Ollama that predates JSON-schema `format` rejects the object form.
      // Plain JSON mode is still constrained decoding; remember and move on.
      structuredOutputRejected = true
      content = try await chat(userContent: userContent, format: "json")
    }

    if let analysis = try? ChunkAnalysisResponseParser.parse(content) { return analysis }

    // One immediate second attempt at a higher temperature: an unusable reply
    // is usually a degenerate decode (a repetition loop that ran into the
    // token budget), and the same prompt at 0.1 tends to reproduce it. A
    // staged converge cannot commit while any chunk lacks a result, so a
    // chunk that fails every pass would hold the whole corpus back.
    let retry = try await chat(
      userContent: userContent, format: structuredOutputRejected ? "json" : ChunkAnalysisPrompt.responseSchema,
      temperature: 0.5
    )
    do {
      return try ChunkAnalysisResponseParser.parse(retry)
    } catch {
      // Thrown, not stored: the store records a retryable failure instead of
      // indexing a fragment of the raw reply as if it were a summary.
      throw OllamaError.invalidResponse(error.localizedDescription)
    }
  }

  /// Whether this server refused a JSON-schema `format` (Ollama < 0.5).
  private var structuredOutputRejected = false

  private func chat(userContent: String, format: Any, temperature: Double = 0.1) async throws -> String {
    let url = URL(string: "\(baseURL)/api/chat")!
    var request = URLRequest(url: url)
    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.timeoutInterval = requestTimeout

    let body: [String: Any] = [
      "model": model,
      "messages": [
        ["role": "system", "content": ChunkAnalysisPrompt.systemPrompt],
        ["role": "user", "content": userContent],
      ],
      "stream": false,
      // Structured output: the reply is decoded against the schema, so it is
      // always one complete JSON object with a summary and tags.
      "format": format,
      // Hold the model resident for the run's duration; server-default
      // keep-alive (5m) is what evicted it between batches (#22).
      "keep_alive": keepAlive,
      "options": [
        "temperature": temperature,
        "num_predict": 512,
      ],
    ]
    request.httpBody = try JSONSerialization.data(withJSONObject: body)

    let data: Data
    let response: URLResponse
    do {
      (data, response) = try await URLSession.shared.data(for: request)
    } catch let error as URLError where error.code == .timedOut {
      // Attributable, not generic: a deadline hit here is almost always the
      // model still paging in, and the corpus hole it causes must say so (#22).
      throw OllamaError.requestTimedOut(model: model, seconds: requestTimeout)
    }

    guard let http = response as? HTTPURLResponse, (200...299).contains(http.statusCode) else {
      let code = (response as? HTTPURLResponse)?.statusCode ?? -1
      throw OllamaError.httpError(code: code, body: "Ollama chat failed")
    }

    guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          let message = json["message"] as? [String: Any],
          let content = message["content"] as? String else {
      throw OllamaError.invalidResponse("Invalid chat response")
    }
    return content
  }
}
