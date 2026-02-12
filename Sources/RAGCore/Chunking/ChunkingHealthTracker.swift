//
//  ChunkingHealthTracker.swift
//  RAGCore
//
//  Tracks chunking failures to enable auto-fallback and diagnostics.
//

import Foundation

/// Tracks chunking failures per file to enable auto-fallback and diagnostics.
///
/// Records failures so problematic files automatically use line-based
/// chunking on re-index instead of retrying AST parsing.
public struct ChunkingHealthTracker: Sendable {

  public enum FailureType: String, Codable, Sendable {
    case timeout = "timeout"
    case crash = "crash"
    case stackOverflow = "stack_overflow"
    case parseError = "parse_error"
    case unknown = "unknown"
  }

  public struct FailureRecord: Codable, Sendable {
    public let filePath: String
    public let language: String
    public let errorType: FailureType
    public let errorMessage: String?
    public let timestamp: Date
    public let fileHash: String

    public init(
      filePath: String, language: String,
      errorType: FailureType, errorMessage: String?,
      timestamp: Date, fileHash: String
    ) {
      self.filePath = filePath
      self.language = language
      self.errorType = errorType
      self.errorMessage = errorMessage
      self.timestamp = timestamp
      self.fileHash = fileHash
    }
  }

  /// URL where failure records are persisted.
  /// Defaults to `~/Library/Application Support/Peel/chunking_failures.json`.
  /// Override via `init(cacheURL:)` for testing or library consumers.
  private let cacheURL: URL

  private var failures: [FailureRecord] = []
  private let maxFailures = 500

  /// Create a tracker that persists to the default app support location.
  public init() {
    let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first!
    let peelDir = appSupport.appendingPathComponent("Peel", isDirectory: true)
    try? FileManager.default.createDirectory(at: peelDir, withIntermediateDirectories: true)
    self.cacheURL = peelDir.appendingPathComponent("chunking_failures.json")
    loadFailures()
  }

  /// Create a tracker with a custom cache URL (useful for testing).
  public init(cacheURL: URL) {
    self.cacheURL = cacheURL
    loadFailures()
  }

  /// Check if AST chunking should be skipped for a file based on previous failures.
  public func shouldSkipAST(for filePath: String, hash: String) -> Bool {
    failures.contains { $0.filePath == filePath && $0.fileHash == hash }
  }

  /// Record a chunking failure.
  public mutating func recordFailure(
    filePath: String,
    language: String,
    errorType: FailureType,
    errorMessage: String?,
    fileHash: String
  ) {
    failures.removeAll { $0.filePath == filePath }

    let record = FailureRecord(
      filePath: filePath,
      language: language,
      errorType: errorType,
      errorMessage: errorMessage,
      timestamp: Date(),
      fileHash: fileHash
    )
    failures.append(record)

    if failures.count > maxFailures {
      failures = Array(failures.suffix(maxFailures))
    }

    saveFailures()
    print("[ChunkingHealth] Recorded failure: \(filePath) - \(errorType.rawValue)")
  }

  /// Clear failures for files that have changed (hash differs).
  public mutating func clearStaleFailures(currentFiles: [(path: String, hash: String)]) {
    let currentMap = Dictionary(uniqueKeysWithValues: currentFiles.map { ($0.path, $0.hash) })
    let before = failures.count
    failures.removeAll { record in
      guard let currentHash = currentMap[record.filePath] else { return true }
      return currentHash != record.fileHash
    }
    let removed = before - failures.count
    if removed > 0 {
      print("[ChunkingHealth] Cleared \(removed) stale failures")
      saveFailures()
    }
  }

  /// Get all current failures for diagnostics.
  public func getFailures() -> [FailureRecord] {
    failures
  }

  /// Get failures grouped by language.
  public func failuresByLanguage() -> [String: Int] {
    Dictionary(grouping: failures, by: { $0.language }).mapValues { $0.count }
  }

  private mutating func loadFailures() {
    guard FileManager.default.fileExists(atPath: cacheURL.path),
          let data = try? Data(contentsOf: cacheURL),
          let decoded = try? JSONDecoder().decode([FailureRecord].self, from: data) else {
      return
    }
    failures = decoded
    print("[ChunkingHealth] Loaded \(failures.count) failure records")
  }

  private func saveFailures() {
    do {
      let data = try JSONEncoder().encode(failures)
      try data.write(to: cacheURL)
    } catch {
      print("[ChunkingHealth] Failed to save: \(error)")
    }
  }
}
