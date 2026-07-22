//
//  OllamaEmbeddingRequestGate.swift
//  RAGCore
//
//  Serializes embedding requests per Ollama endpoint/model and prevents a
//  crashed llama-server runner from being relaunched in a tight retry loop.
//

import Foundation

/// Raised when repeated transient Ollama failures open the embedding circuit.
/// Callers can surface this once and avoid treating every blocked retry as a
/// separate backend incident.
public struct OllamaEmbeddingCircuitOpenError: LocalizedError, Sendable {
  public let endpoint: String
  public let model: String
  public let retryAfter: Date
  public let consecutiveFailures: Int
  public let openedNow: Bool

  public var errorDescription: String? {
    let retry = ISO8601DateFormatter().string(from: retryAfter)
    return "Ollama embedding circuit is open for \(model) at \(endpoint) after "
      + "\(consecutiveFailures) transient failures; retry after \(retry)."
  }
}

/// One request at a time per endpoint/model plus a bounded failure circuit.
///
/// `actor` reentrancy normally allows another caller to enter while network I/O
/// is suspended. The explicit per-key permit keeps the actual request bodies
/// serialized as well, so multiple indexing/enrichment jobs cannot pile onto
/// the same llama-server runner.
actor OllamaEmbeddingRequestGate {
  struct Key: Hashable, Sendable {
    let endpoint: String
    let model: String
  }

  private struct CircuitState {
    var consecutiveFailures = 0
    var openCount = 0
    var openUntil: Date?
  }

  static let shared = OllamaEmbeddingRequestGate()

  private let failureThreshold: Int
  private let initialCooldown: TimeInterval
  private let maximumCooldown: TimeInterval
  private var busy: Set<Key> = []
  private var waiters: [Key: [CheckedContinuation<Void, Never>]] = [:]
  private var circuits: [Key: CircuitState] = [:]

  init(
    failureThreshold: Int = 3,
    initialCooldown: TimeInterval = 5 * 60,
    maximumCooldown: TimeInterval = 60 * 60
  ) {
    self.failureThreshold = max(failureThreshold, 1)
    self.initialCooldown = max(initialCooldown, 1)
    self.maximumCooldown = max(maximumCooldown, initialCooldown)
  }

  func perform<Value: Sendable>(
    endpoint: String,
    model: String,
    now: Date = Date(),
    operation: @Sendable @escaping () async throws -> Value
  ) async throws -> Value {
    let key = Key(endpoint: endpoint, model: model)
    await acquire(key)
    defer { release(key) }
    try Task.checkCancellation()

    var circuit = circuits[key] ?? CircuitState()
    if let openUntil = circuit.openUntil, openUntil > now {
      throw OllamaEmbeddingCircuitOpenError(
        endpoint: endpoint,
        model: model,
        retryAfter: openUntil,
        consecutiveFailures: circuit.consecutiveFailures,
        openedNow: false
      )
    }

    // Once the cooldown expires, admit exactly one half-open probe. Because the
    // per-key permit remains held, every other caller waits for its outcome.
    let isHalfOpenProbe = circuit.openUntil != nil
    if isHalfOpenProbe {
      circuit.openUntil = nil
      circuits[key] = circuit
    }

    do {
      let value = try await operation()
      circuits[key] = nil
      return value
    } catch {
      if error is CancellationError || Task.isCancelled {
        throw error
      }
      guard Self.countsTowardCircuit(error) else {
        throw error
      }

      circuit = circuits[key] ?? circuit
      circuit.consecutiveFailures += 1
      let shouldOpen = isHalfOpenProbe
        || circuit.consecutiveFailures >= failureThreshold
      guard shouldOpen else {
        circuits[key] = circuit
        throw error
      }

      circuit.openCount += 1
      let exponent = min(max(circuit.openCount - 1, 0), 10)
      let cooldown = min(
        initialCooldown * pow(2, Double(exponent)),
        maximumCooldown
      )
      let retryAfter = now.addingTimeInterval(cooldown)
      circuit.openUntil = retryAfter
      circuits[key] = circuit
      throw OllamaEmbeddingCircuitOpenError(
        endpoint: endpoint,
        model: model,
        retryAfter: retryAfter,
        consecutiveFailures: circuit.consecutiveFailures,
        openedNow: true
      )
    }
  }

  private func acquire(_ key: Key) async {
    if busy.insert(key).inserted {
      return
    }
    await withCheckedContinuation { continuation in
      waiters[key, default: []].append(continuation)
    }
  }

  private func release(_ key: Key) {
    guard var queued = waiters[key], !queued.isEmpty else {
      waiters[key] = nil
      busy.remove(key)
      return
    }
    let next = queued.removeFirst()
    waiters[key] = queued.isEmpty ? nil : queued
    next.resume()
  }

  private static func countsTowardCircuit(_ error: Error) -> Bool {
    if let urlError = error as? URLError {
      return [
        .cannotConnectToHost,
        .cannotFindHost,
        .dnsLookupFailed,
        .networkConnectionLost,
        .timedOut,
      ].contains(urlError.code)
    }
    if let ollamaError = error as? OllamaError {
      switch ollamaError {
      case .httpError(let code, _):
        return code >= 500
      case .invalidResponse:
        return true
      case .modelNotAvailable:
        return false
      }
    }
    return false
  }
}
