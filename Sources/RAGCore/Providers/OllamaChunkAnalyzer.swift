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

  /// Create an analyzer for a specific Ollama model.
  /// - Parameters:
  ///   - model: Ollama model name (e.g. "gemma3:12b", "qwen3:8b")
  ///   - baseURL: Ollama API base URL (default: http://localhost:11434)
  public init(model: String, baseURL: String = "http://localhost:11434") {
    self.model = model
    self.analyzerName = model
    self.baseURL = baseURL
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
    request.timeoutInterval = 60

    // Qwen3 extended thinking mode produces verbose output — disable it
    let isQwen3 = model.lowercased().hasPrefix("qwen3") && !model.lowercased().contains("embed")
    let userContent = isQwen3 ? "/no_think\n\(prompt)" : prompt

    let body: [String: Any] = [
      "model": model,
      "messages": [
        ["role": "system", "content": systemPrompt],
        ["role": "user", "content": userContent],
      ],
      "stream": false,
      "options": [
        "temperature": 0.1,
        "num_predict": 512,
      ],
    ]
    request.httpBody = try JSONSerialization.data(withJSONObject: body)

    let (data, response) = try await URLSession.shared.data(for: request)

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
