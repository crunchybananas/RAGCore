@testable import RAGCore
import Foundation
import Testing

private actor EmbeddingStartSignal {
  private var started = false
  private var continuations: [CheckedContinuation<Void, Never>] = []

  func wait() async {
    guard !started else { return }
    await withCheckedContinuation { continuation in
      continuations.append(continuation)
    }
  }

  func fire() {
    guard !started else { return }
    started = true
    let pending = continuations
    continuations.removeAll()
    for continuation in pending {
      continuation.resume()
    }
  }
}

private struct CancellableEmbeddingProvider: EmbeddingProvider {
  let signal: EmbeddingStartSignal
  let dimensions = 3
  let modelName = "cancellation-test"

  func embed(texts: [String]) async throws -> [[Float]] {
    await signal.fire()
    try await Task.sleep(for: .seconds(30))
    return texts.map { _ in [0, 0, 0] }
  }
}

@Suite("Index cancellation")
struct IndexCancellationTests {
  @Test("A cancelled scanner stops before walking the repository")
  func scannerHonorsCancellation() async throws {
    let rootURL = try makeRepository()
    defer { try? FileManager.default.removeItem(at: rootURL) }

    let scanTask = Task {
      while !Task.isCancelled {
        await Task.yield()
      }
      return try RAGFileScanner().scanCancellable(rootURL: rootURL)
    }
    scanTask.cancel()

    do {
      _ = try await scanTask.value
      Issue.record("Expected the cancelled scanner to throw CancellationError")
    } catch is CancellationError {
      // Expected.
    } catch {
      Issue.record("Expected CancellationError, got \(error)")
    }
  }

  @Test("Cancellation from the embedding provider ends the index run")
  func embeddingCancellationPropagates() async throws {
    let rootURL = try makeRepository()
    let databaseURL = rootURL.appendingPathComponent("rag.sqlite")
    let signal = EmbeddingStartSignal()
    let store = RAGStore(
      dbURL: databaseURL,
      embeddingProvider: CancellableEmbeddingProvider(signal: signal)
    )

    let indexTask = Task {
      try await store.indexRepository(
        path: rootURL.path,
        forceReindex: true,
        allowWorkspace: false,
        excludeSubrepos: true,
        progress: nil
      )
    }

    await signal.wait()
    indexTask.cancel()

    do {
      _ = try await indexTask.value
      Issue.record("Expected the cancelled index run to throw CancellationError")
    } catch is CancellationError {
      // Expected.
    } catch {
      Issue.record("Expected CancellationError, got \(error)")
    }

    await store.closeDatabase()
    try? FileManager.default.removeItem(at: rootURL)
  }

  private func makeRepository() throws -> URL {
    let rootURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-index-cancellation-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
    let sourceURL = rootURL.appendingPathComponent("CancellationProbe.swift")
    try "func cancellationProbe() -> Int { 42 }\n".write(to: sourceURL, atomically: true, encoding: .utf8)
    return rootURL
  }
}
