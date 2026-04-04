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

  public var errorDescription: String? {
    switch self {
    case .httpError(let code, let body):
      return "Ollama HTTP \(code): \(body)"
    case .invalidResponse(let detail):
      return "Invalid Ollama response: \(detail)"
    case .modelNotAvailable(let model):
      return "Ollama model '\(model)' is not available"
    }
  }
}
