//
//  OllamaError.swift
//  RAGCore
//
//  Shared error types for Ollama providers.
//

import Foundation

/// Errors from Ollama API calls.
public enum OllamaError: LocalizedError {
  case httpError(code: Int, body: String)
  case invalidResponse(String)
  case modelNotAvailable(String)
  /// The request hit the client-side deadline before Ollama answered. On an
  /// analyzer this is almost always a cold model load (a 17-30 GB model takes
  /// minutes to page back in after eviction), which is why it gets its own
  /// case: a corpus hole caused by "the model was not loaded yet" must be
  /// attributable as such, not filed as a generic analysis failure (#22).
  case requestTimedOut(model: String, seconds: Double)

  public var errorDescription: String? {
    switch self {
    case .httpError(let code, let body):
      return "Ollama HTTP \(code): \(body)"
    case .invalidResponse(let detail):
      return "Invalid Ollama response: \(detail)"
    case .modelNotAvailable(let model):
      return "Ollama model '\(model)' is not available"
    case .requestTimedOut(let model, let seconds):
      return "Ollama request for '\(model)' timed out after \(Int(seconds))s — "
        + "commonly a cold model load still paging in"
    }
  }
}
