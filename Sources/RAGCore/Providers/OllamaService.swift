//
//  OllamaService.swift
//  RAGCore
//
//  Utility for checking Ollama availability and listing models.
//

import Foundation

/// Utility for interacting with a local Ollama instance.
public enum OllamaService {
  /// Check if Ollama is running and reachable.
  /// - Parameter baseURL: Ollama API base URL (default: http://localhost:11434)
  /// - Returns: true if Ollama responds within the timeout
  public static func isAvailable(baseURL: String = "http://localhost:11434", timeout: TimeInterval = 3) async -> Bool {
    guard let url = URL(string: "\(baseURL)/api/tags") else { return false }
    var request = URLRequest(url: url)
    request.timeoutInterval = timeout
    do {
      let (_, response) = try await URLSession.shared.data(for: request)
      return (response as? HTTPURLResponse)?.statusCode == 200
    } catch {
      return false
    }
  }

  /// List installed Ollama models.
  /// - Parameter baseURL: Ollama API base URL
  /// - Returns: Array of model names, or empty if Ollama is unavailable
  public static func listModels(baseURL: String = "http://localhost:11434") async -> [String] {
    guard let url = URL(string: "\(baseURL)/api/tags") else { return [] }
    do {
      let (data, _) = try await URLSession.shared.data(from: url)
      guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
            let models = json["models"] as? [[String: Any]] else { return [] }
      return models.compactMap { $0["name"] as? String }
    } catch {
      return []
    }
  }

  /// List installed embedding models (filters by known embedding model names).
  public static func listEmbeddingModels(baseURL: String = "http://localhost:11434") async -> [String] {
    let all = await listModels(baseURL: baseURL)
    let embeddingPrefixes = ["nomic-embed", "mxbai-embed", "all-minilm", "snowflake-arctic", "qwen3-embedding"]
    return all.filter { model in
      embeddingPrefixes.contains(where: { model.hasPrefix($0) })
    }
  }
}
