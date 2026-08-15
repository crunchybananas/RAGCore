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
    let prompt = buildPrompt(chunk: chunk, constructType: constructType, constructName: constructName, language: language)

    let systemPrompt = """
    You are a code analyzer. Given a code chunk, produce a JSON object with:
    - "summary": A concise 1-2 sentence description of what this code does and why.
    - "tags": An array of 2-5 semantic tags (lowercase, hyphenated) describing the code's purpose.

    Respond with ONLY the JSON object, no markdown fences, no explanation.
    Example: {"summary": "Validates user email format and checks for duplicates", "tags": ["validation", "email", "user-input"]}
    """

    let url = URL(string: "\(baseURL)/api/chat")!
    var request = URLRequest(url: url)
    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.timeoutInterval = requestTimeout

    // Qwen3 extended thinking mode produces verbose output — disable it
    let userContent = Self.wantsNoThinkPrefix(model: model) ? "/no_think\n\(prompt)" : prompt

    let body: [String: Any] = [
      "model": model,
      "messages": [
        ["role": "system", "content": systemPrompt],
        ["role": "user", "content": userContent],
      ],
      "stream": false,
      // Hold the model resident for the run's duration; server-default
      // keep-alive (5m) is what evicted it between batches (#22).
      "keep_alive": keepAlive,
      "options": [
        "temperature": 0.1,
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
          var content = message["content"] as? String else {
      throw OllamaError.invalidResponse("Invalid chat response")
    }

    // Strip thinking tags if present (Qwen3)
    if content.contains("<think>") {
      content = content.replacingOccurrences(
        of: #"<think>[\s\S]*?</think>"#, with: "", options: .regularExpression
      ).trimmingCharacters(in: .whitespacesAndNewlines)
    }

    return parseAnalysis(content)
  }

  private func buildPrompt(chunk: String, constructType: String?, constructName: String?, language: String?) -> String {
    var parts: [String] = []
    if let lang = language { parts.append("Language: \(lang)") }
    if let ct = constructType { parts.append("Type: \(ct)") }
    if let cn = constructName { parts.append("Name: \(cn)") }
    parts.append("Code:\n\(String(chunk.prefix(3000)))")
    return parts.joined(separator: "\n")
  }

  private func parseAnalysis(_ content: String) -> ChunkAnalysis {
    let trimmed = content.trimmingCharacters(in: .whitespacesAndNewlines)

    // Strip markdown fences if present
    var jsonStr = trimmed
    if jsonStr.hasPrefix("```") {
      jsonStr = jsonStr.replacingOccurrences(of: #"```(?:json)?\n?"#, with: "", options: .regularExpression)
    }

    if let data = jsonStr.data(using: .utf8),
       let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
       let summary = json["summary"] as? String {
      let tags = (json["tags"] as? [String]) ?? []
      return ChunkAnalysis(summary: summary, tags: tags)
    }

    // Fallback: use the raw text as summary
    return ChunkAnalysis(summary: String(trimmed.prefix(200)), tags: [])
  }
}
