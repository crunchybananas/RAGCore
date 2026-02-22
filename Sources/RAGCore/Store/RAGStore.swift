//
//  RAGStore.swift
//  RAGCore
//
//  Core actor for RAG storage, indexing, and search operations.
//  Manages a SQLite database with vector embeddings for semantic code search.
//

import CryptoKit
import CSQLite
import Darwin
import Foundation
import MachO

/// Core actor for RAG (Retrieval-Augmented Generation) storage.
///
/// Manages:
/// - SQLite database with schema migrations (v1→v13)
/// - File scanning and chunking pipeline (AST + line-based)
/// - Embedding generation and caching
/// - Vector search (accelerated via sqlite-vec, or brute-force fallback)
/// - Text search (FTS5-based)
/// - Dependency graph (import/inheritance tracking)
/// - Lessons learned (agent mistake → fix patterns)
/// - Query hints (search analytics)
///
/// ## Usage
/// ```swift
/// let store = RAGStore(
///   embeddingProvider: myProvider,
///   chunkAnalyzer: myAnalyzer,     // optional
///   memoryMonitor: myMonitor       // optional
/// )
/// let status = try store.initialize()
/// let report = try await store.indexRepository(path: "/path/to/repo")
/// let results = try await store.searchVector(query: "authentication", repoPath: "/path/to/repo")
/// ```
public actor RAGStore {

  // MARK: - Public Types

  public struct Status: Sendable {
    public let dbPath: String
    public let exists: Bool
    public let schemaVersion: Int
    public let extensionLoaded: Bool
    public let lastInitializedAt: Date?
    public let providerName: String
    public let embeddingModelName: String
    public let embeddingDimensions: Int
  }

  public struct Stats: Sendable {
    public let repoCount: Int
    public let fileCount: Int
    public let chunkCount: Int
    public let embeddingCount: Int
    public let cacheEmbeddingCount: Int
    public let dbSizeBytes: Int
    public let lastIndexedAt: Date?
    public let lastIndexedRepoPath: String?
  }

  public enum RAGError: LocalizedError {
    case sqlite(String)
    case invalidPath
    case workspaceDetected(rootPath: String, repoPaths: [String])
    case embeddingFailed(String)

    public var errorDescription: String? {
      switch self {
      case .sqlite(let message):
        return message
      case .invalidPath:
        return "Invalid database path"
      case .workspaceDetected(let rootPath, let repoPaths):
        let preview = repoPaths.prefix(6).joined(separator: "\n")
        let suffix = repoPaths.count > 6 ? "\n…" : ""
        return "Workspace detected at \(rootPath). Index sub-repos instead:\n\(preview)\(suffix)"
      case .embeddingFailed(let message):
        return "Embedding failed: \(message)"
      }
    }
  }

  public struct RepoInfo: Sendable {
    public let id: String
    public let name: String
    public let rootPath: String
    public let lastIndexedAt: Date?
    public let fileCount: Int
    public let chunkCount: Int
    public let repoIdentifier: String?
    public let parentRepoId: String?
  }

  public struct ChunkingHealthInfo: Sendable {
    public let totalFailures: Int
    public let failuresByLanguage: [String: Int]
    public let recentFailures: [(path: String, language: String, errorType: String, timestamp: Date)]
  }

  /// Facet counts for filtering/grouping search results.
  public struct FacetCounts: Sendable {
    public let modulePaths: [(path: String, count: Int)]
    public let featureTags: [(tag: String, count: Int)]
    public let languages: [(language: String, count: Int)]
    public let constructTypes: [(type: String, count: Int)]
  }

  /// A duplicate code group found across multiple files.
  public struct DuplicateGroup: Sendable {
    public let constructName: String
    public let constructType: String
    public let fileCount: Int
    public let totalTokens: Int
    public let wastedTokens: Int
    public let aiSummary: String?
    public let files: [(path: String, tokenCount: Int)]
  }

  /// Naming pattern group result.
  public struct PatternGroup: Sendable {
    public let suffix: String
    public let count: Int
    public let totalTokens: Int
    public let samples: [(constructName: String, path: String, tokenCount: Int)]
  }

  /// A construct exceeding a token threshold — refactoring candidate.
  public struct Hotspot: Sendable {
    public let constructName: String
    public let constructType: String
    public let filePath: String
    public let tokenCount: Int
    public let startLine: Int
    public let endLine: Int
    public let aiSummary: String?
    public let aiTags: [String]
  }

  /// Cached AI analysis result for a text chunk.
  struct CachedAIAnalysis {
    let summary: String
    let tags: String
    let model: String
  }

  // MARK: - Internal State

  internal let dbURL: URL
  internal var db: OpaquePointer?
  internal var schemaVersion: Int = 0
  internal var extensionLoaded: Bool = false
  internal var lastInitializedAt: Date?
  internal let sqliteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

  internal let scanner: RAGFileScanner
  internal let chunker: HybridChunker
  internal let embeddingProvider: EmbeddingProvider
  internal let chunkAnalyzer: ChunkAnalyzer?
  internal let memoryMonitor: MemoryPressureMonitor
  internal var healthTracker: ChunkingHealthTracker

  internal let dateFormatter = ISO8601DateFormatter()

  /// Cache for git remote URL lookups to avoid repeated subprocess calls.
  internal var remoteURLCache: [String: String?] = [:]

  /// Package manifest filenames for sub-package detection.
  static let packageManifests: Set<String> = [
    "Package.swift",
    "package.json",
    "Cargo.toml",
    "go.mod",
    "pyproject.toml",
    "Gemfile",
    "build.gradle",
    "build.gradle.kts",
    "pom.xml",
    "mix.exs",
    "pubspec.yaml",
    "CMakeLists.txt",
  ]

  // MARK: - Init

  /// Create a new RAG store.
  ///
  /// - Parameters:
  ///   - dbURL: URL for the SQLite database file. If nil, uses the default Application Support location.
  ///   - embeddingProvider: Provider for generating vector embeddings.
  ///   - chunkAnalyzer: Optional analyzer for AI-powered chunk analysis.
  ///   - memoryMonitor: Optional memory pressure monitor. Defaults to `NoOpMemoryMonitor`.
  ///   - astChunkerCLIPath: Explicit path to `ast-chunker-cli`. If nil, auto-discovered.
  ///   - astChunkerSearchPaths: Additional paths to search for `ast-chunker-cli`.
  ///   - scanner: File scanner to use. If nil, uses default.
  public init(
    dbURL: URL? = nil,
    embeddingProvider: EmbeddingProvider,
    chunkAnalyzer: ChunkAnalyzer? = nil,
    memoryMonitor: MemoryPressureMonitor? = nil,
    astChunkerCLIPath: String? = nil,
    astChunkerSearchPaths: [String] = [],
    scanner: RAGFileScanner? = nil
  ) {
    self.embeddingProvider = embeddingProvider
    self.chunkAnalyzer = chunkAnalyzer
    self.memoryMonitor = memoryMonitor ?? NoOpMemoryMonitor()
    self.healthTracker = ChunkingHealthTracker()
    self.chunker = HybridChunker(astChunkerCLIPath: astChunkerCLIPath, searchPaths: astChunkerSearchPaths)
    self.scanner = scanner ?? RAGFileScanner()

    if let dbURL {
      self.dbURL = dbURL
    } else {
      let baseURL = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
        ?? FileManager.default.temporaryDirectory
      let ragURL = baseURL.appendingPathComponent("Peel/RAG", isDirectory: true)
      if !FileManager.default.fileExists(atPath: ragURL.path) {
        try? FileManager.default.createDirectory(at: ragURL, withIntermediateDirectories: true)
      }
      self.dbURL = ragURL.appendingPathComponent("rag.sqlite")
    }
  }

  // MARK: - Lifecycle

  /// Initialize the database, loading extensions and ensuring schema is up to date.
  public func initialize(extensionPath: String? = nil) throws -> Status {
    try openIfNeeded()
    try loadExtensionIfAvailable(extensionPath: extensionPath)
    try ensureSchema()
    lastInitializedAt = Date()
    return status()
  }

  /// Current status of the store.
  public func status() -> Status {
    return Status(
      dbPath: dbURL.path,
      exists: FileManager.default.fileExists(atPath: dbURL.path),
      schemaVersion: schemaVersion,
      extensionLoaded: extensionLoaded,
      lastInitializedAt: lastInitializedAt,
      providerName: String(describing: type(of: embeddingProvider)),
      embeddingModelName: embeddingProvider.modelName,
      embeddingDimensions: embeddingProvider.dimensions
    )
  }

  /// Aggregate stats about the database.
  public func stats() throws -> Stats {
    try openIfNeeded()
    try ensureSchema()

    let repoCount = try queryInt("SELECT COUNT(*) FROM repos")
    let fileCount = try queryInt("SELECT COUNT(*) FROM files")
    let chunkCount = try queryInt("SELECT COUNT(*) FROM chunks")
    let embeddingCount = try queryInt("SELECT COUNT(*) FROM embeddings")
    let cacheEmbeddingCount = try queryInt("SELECT COUNT(*) FROM cache_embeddings")

    let lastIndexedRow = try queryRow(
      "SELECT root_path, last_indexed_at FROM repos WHERE last_indexed_at IS NOT NULL ORDER BY last_indexed_at DESC LIMIT 1"
    )
    let lastIndexedRepoPath = lastIndexedRow?.0
    let lastIndexedAt = lastIndexedRow.flatMap { dateFormatter.date(from: $0.1) }

    let dbSizeBytes = (try? FileManager.default.attributesOfItem(atPath: dbURL.path)[.size] as? NSNumber)?.intValue ?? 0

    return Stats(
      repoCount: repoCount,
      fileCount: fileCount,
      chunkCount: chunkCount,
      embeddingCount: embeddingCount,
      cacheEmbeddingCount: cacheEmbeddingCount,
      dbSizeBytes: dbSizeBytes,
      lastIndexedAt: lastIndexedAt,
      lastIndexedRepoPath: lastIndexedRepoPath
    )
  }

  /// Close the underlying SQLite database.
  public func closeDatabase() {
    if let handle = db {
      sqlite3_close(handle)
      db = nil
    }
  }

  /// List all indexed repositories.
  public func listRepos() throws -> [RepoInfo] {
    try openIfNeeded()
    try ensureSchema()

    let sql = """
      SELECT r.id, r.name, r.root_path, r.last_indexed_at,
             (SELECT COUNT(*) FROM files WHERE repo_id = r.id) as file_count,
             (SELECT COUNT(*) FROM chunks c JOIN files f ON c.file_id = f.id WHERE f.repo_id = r.id) as chunk_count,
             r.repo_identifier,
             r.parent_repo_id
      FROM repos r
      ORDER BY r.name
      """

    guard let db else {
      throw RAGError.sqlite("Database not initialized")
    }

    var statement: OpaquePointer?
    let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
    guard result == SQLITE_OK, let statement else {
      let message = String(cString: sqlite3_errmsg(db))
      throw RAGError.sqlite(message)
    }
    defer { sqlite3_finalize(statement) }

    var repos: [RepoInfo] = []
    while sqlite3_step(statement) == SQLITE_ROW {
      let id = String(cString: sqlite3_column_text(statement, 0))
      let name = String(cString: sqlite3_column_text(statement, 1))
      let rootPath = String(cString: sqlite3_column_text(statement, 2))
      let lastIndexedAtStr = sqlite3_column_text(statement, 3).map { String(cString: $0) }
      let lastIndexedAt = lastIndexedAtStr.flatMap { dateFormatter.date(from: $0) }
      let fileCount = Int(sqlite3_column_int(statement, 4))
      let chunkCount = Int(sqlite3_column_int(statement, 5))
      let repoIdentifier = sqlite3_column_text(statement, 6).map { String(cString: $0) }
      let parentRepoId = sqlite3_column_text(statement, 7).map { String(cString: $0) }

      repos.append(RepoInfo(
        id: id, name: name, rootPath: rootPath, lastIndexedAt: lastIndexedAt,
        fileCount: fileCount, chunkCount: chunkCount,
        repoIdentifier: repoIdentifier, parentRepoId: parentRepoId
      ))
    }
    return repos
  }

  /// Delete a repo and all its associated data (files, chunks, embeddings).
  public func deleteRepo(repoId: String? = nil, repoPath: String? = nil) throws -> Int {
    try openIfNeeded()
    try ensureSchema()

    guard let db else {
      throw RAGError.sqlite("Database not initialized")
    }

    let targetId: String
    if let repoId {
      targetId = repoId
    } else if let repoPath {
      targetId = try resolveRepoId(for: repoPath)
    } else {
      throw RAGError.sqlite("Must provide repoId or repoPath")
    }

    print("[RAG] deleteRepo: targetId=\(targetId)")

    // Helper to execute a DELETE and check for errors
    func execDelete(_ sql: String, label: String) throws {
      var stmt: OpaquePointer?
      let prepareResult = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
      guard prepareResult == SQLITE_OK, let stmt else {
        let msg = String(cString: sqlite3_errmsg(db))
        print("[RAG] deleteRepo: PREPARE failed for \(label): \(msg)")
        throw RAGError.sqlite("deleteRepo \(label) prepare failed: \(msg)")
      }
      defer { sqlite3_finalize(stmt) }
      sqlite3_bind_text(stmt, 1, targetId, -1, sqliteTransient)
      let stepResult = sqlite3_step(stmt)
      if stepResult != SQLITE_DONE {
        let msg = String(cString: sqlite3_errmsg(db))
        print("[RAG] deleteRepo: STEP failed for \(label): code=\(stepResult) msg=\(msg)")
        throw RAGError.sqlite("deleteRepo \(label) failed: \(msg)")
      }
      let changes = sqlite3_changes(db)
      print("[RAG] deleteRepo: \(label) removed \(changes) rows")
    }

    // Count files before deletion
    let countSql = "SELECT COUNT(*) FROM files WHERE repo_id = ?"
    var countStmt: OpaquePointer?
    sqlite3_prepare_v2(db, countSql, -1, &countStmt, nil)
    defer { sqlite3_finalize(countStmt) }
    sqlite3_bind_text(countStmt, 1, targetId, -1, sqliteTransient)
    var deletedFiles = 0
    if sqlite3_step(countStmt) == SQLITE_ROW {
      deletedFiles = Int(sqlite3_column_int(countStmt, 0))
    }
    print("[RAG] deleteRepo: repo has \(deletedFiles) files to delete")

    // Delete from vec_chunks first (virtual table - CASCADE doesn't apply)
    if extensionLoaded {
      try execDelete("""
        DELETE FROM vec_chunks WHERE chunk_id IN (
          SELECT c.id FROM chunks c
          JOIN files f ON c.file_id = f.id
          WHERE f.repo_id = ?
        )
        """, label: "vec_chunks")
    }

    // Delete dependencies by repo_id
    try execDelete("DELETE FROM dependencies WHERE repo_id = ?", label: "dependencies")

    // Delete symbol_refs by repo_id
    try execDelete("DELETE FROM symbol_refs WHERE repo_id = ?", label: "symbol_refs")

    // Delete embeddings
    try execDelete("""
      DELETE FROM embeddings WHERE chunk_id IN (
        SELECT c.id FROM chunks c
        JOIN files f ON c.file_id = f.id
        WHERE f.repo_id = ?
      )
      """, label: "embeddings")

    // Delete chunks
    try execDelete("""
      DELETE FROM chunks WHERE file_id IN (
        SELECT id FROM files WHERE repo_id = ?
      )
      """, label: "chunks")

    // Delete files
    try execDelete("DELETE FROM files WHERE repo_id = ?", label: "files")

    // Delete repo
    try execDelete("DELETE FROM repos WHERE id = ?", label: "repo")

    print("[RAG] deleteRepo: completed, \(deletedFiles) files removed")
    return deletedFiles
  }

  /// Get chunking health info.
  public func getChunkingHealth() -> ChunkingHealthInfo {
    let failures = healthTracker.getFailures()
    let byLanguage = healthTracker.failuresByLanguage()
    let recent = failures.suffix(20).map { (
      path: $0.filePath,
      language: $0.language,
      errorType: $0.errorType.rawValue,
      timestamp: $0.timestamp
    ) }
    return ChunkingHealthInfo(
      totalFailures: failures.count,
      failuresByLanguage: byLanguage,
      recentFailures: recent
    )
  }

  /// Clear chunking failures (useful after code changes).
  public func clearChunkingFailures() {
    healthTracker = ChunkingHealthTracker()
  }

  /// Clear the embedding cache.
  public func clearEmbeddingCache() throws -> Int {
    try openIfNeeded()
    try ensureSchema()
    let cleared = try queryInt("SELECT COUNT(*) FROM cache_embeddings")
    try exec("DELETE FROM cache_embeddings")
    return cleared
  }

  /// Generate embeddings for the given texts using the configured provider.
  public func generateEmbeddings(for texts: [String]) async throws -> [[Float]] {
    try await embeddingProvider.embed(texts: texts)
  }

  // MARK: - Memory Diagnostics

  internal func logMemory(_ label: String) {
    var info = mach_task_basic_info()
    var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size) / 4
    let result = withUnsafeMutablePointer(to: &info) { pointer in
      pointer.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { rebound in
        task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), rebound, &count)
      }
    }

    guard result == KERN_SUCCESS else {
      print("[RAG] Memory \(label): unavailable")
      return
    }

    let rss = ByteCountFormatter.string(fromByteCount: Int64(info.resident_size), countStyle: .memory)
    let vms = ByteCountFormatter.string(fromByteCount: Int64(info.virtual_size), countStyle: .memory)
    print("[RAG] Memory \(label): RSS \(rss), VMS \(vms)")
    let memDesc = memoryMonitor.memoryDescription()
    if !memDesc.isEmpty {
      print("[RAG] Provider Memory \(label): \(memDesc)")
    }
  }

  // MARK: - Test File Detection

  /// Detect if a file path indicates a test file.
  internal func isTestFile(_ path: String) -> Bool {
    let lowercased = path.lowercased()
    return lowercased.contains("/test/") ||
      lowercased.contains("/tests/") ||
      lowercased.contains("/spec/") ||
      lowercased.contains("_test.") ||
      lowercased.contains("-test.") ||
      lowercased.contains("_spec.") ||
      lowercased.contains("-spec.") ||
      lowercased.contains(".test.") ||
      lowercased.contains(".spec.")
  }

  // MARK: - Workspace Detection

  internal func detectWorkspaceRepos(rootURL: URL) -> [String] {
    let resolvedRoot = rootURL.resolvingSymlinksInPath()
    guard let enumerator = FileManager.default.enumerator(
      at: resolvedRoot,
      includingPropertiesForKeys: [.isDirectoryKey],
      options: [.skipsHiddenFiles]
    ) else {
      return []
    }

    let excluded = Set([".git", ".build", ".swiftpm", "build", "dist", "DerivedData", "node_modules", "coverage", "tmp", "Carthage", ".turbo", "__snapshots__", "vendor"])
    let baseDepth = resolvedRoot.pathComponents.count
    var repos: [String] = []

    for case let url as URL in enumerator {
      let depth = url.pathComponents.count - baseDepth
      if depth <= 0 { continue }
      if depth > 4 { enumerator.skipDescendants(); continue }
      if excluded.contains(url.lastPathComponent) { enumerator.skipDescendants(); continue }
      guard (try? url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) == true else { continue }
      if isGitRepo(at: url) {
        repos.append(url.path)
        enumerator.skipDescendants()
      }
    }

    return Array(Set(repos)).sorted()
  }

  internal func detectSubPackages(rootURL: URL, excludingGitRepos gitRepos: [String]) -> [String] {
    let resolvedRoot = rootURL.resolvingSymlinksInPath()
    guard let enumerator = FileManager.default.enumerator(
      at: resolvedRoot,
      includingPropertiesForKeys: [.isDirectoryKey],
      options: [.skipsHiddenFiles]
    ) else {
      return []
    }

    let excluded = Set([".git", ".build", ".swiftpm", "build", "dist", "DerivedData", "node_modules", "coverage", "tmp", "Carthage", ".turbo", "__snapshots__", "vendor"])
    let baseDepth = resolvedRoot.pathComponents.count
    let rootPath = resolvedRoot.path
    var packages: [String] = []
    let gitRepoSet = Set(gitRepos)

    for case let url as URL in enumerator {
      let depth = url.pathComponents.count - baseDepth
      if depth <= 0 { continue }
      if depth > 4 { enumerator.skipDescendants(); continue }
      let lastName = url.lastPathComponent
      if excluded.contains(lastName) { enumerator.skipDescendants(); continue }

      let urlPath = url.path
      if gitRepoSet.contains(where: { urlPath == $0 || urlPath.hasPrefix($0 + "/") }) {
        enumerator.skipDescendants()
        continue
      }

      if Self.packageManifests.contains(lastName) {
        let parentDir = url.deletingLastPathComponent().path
        if parentDir != rootPath {
          packages.append(parentDir)
        }
      }
    }

    return Array(Set(packages)).sorted()
  }

  internal func isGitRepo(at url: URL) -> Bool {
    let gitURL = url.appendingPathComponent(".git")
    var isDir = ObjCBool(false)
    let exists = FileManager.default.fileExists(atPath: gitURL.path, isDirectory: &isDir)
    if exists { return true }
    return FileManager.default.fileExists(atPath: gitURL.path)
  }

  // MARK: - Facet Extraction

  /// Extract module path from file path.
  internal func extractModulePath(from path: String) -> String? {
    let components = path.split(separator: "/").map(String.init)
    guard components.count > 1 else { return nil }
    let directory = components.dropLast().joined(separator: "/")
    let parts = directory.split(separator: "/").prefix(2).map(String.init)
    return parts.isEmpty ? nil : parts.joined(separator: "/")
  }

  /// Extract feature tags from file path, language, and chunk metadata.
  internal func extractFeatureTags(from path: String, language: String, chunks: [RAGChunk]) -> [String] {
    var tags = Set<String>()
    let lowercasedPath = path.lowercased()

    let keywords = [
      "rag", "mcp", "agent", "swarm", "git", "github", "brew", "service",
      "view", "model", "handler", "tool", "embed", "index", "search",
      "chunk", "ast", "vm", "distributed", "worktree", "chain", "template", "config",
    ]
    for keyword in keywords {
      if lowercasedPath.contains(keyword) { tags.insert(keyword) }
    }

    for chunk in chunks {
      if let metadataJson = chunk.metadata,
         let data = metadataJson.data(using: .utf8),
         let metadata = try? JSONDecoder().decode(ChunkMetadataForFacets.self, from: data) {
        for framework in metadata.frameworks ?? [] {
          tags.insert(framework.lowercased())
        }
        if metadata.usesEmberConcurrency == true { tags.insert("ember-concurrency") }
        if metadata.hasTemplate == true { tags.insert("glimmer") }
      }
    }

    tags.insert(language.lowercased())
    return tags.sorted()
  }

  // MARK: - Portable Repo Identifier

  /// Normalize a git remote URL to a canonical form for cross-machine comparison.
  public static func normalizeGitRemoteURL(_ url: String) -> String {
    var normalized = url.trimmingCharacters(in: .whitespacesAndNewlines)
    if normalized.hasSuffix(".git") {
      normalized = String(normalized.dropLast(4))
    }
    if normalized.hasPrefix("git@") {
      normalized = normalized
        .replacingOccurrences(of: "git@", with: "")
        .replacingOccurrences(of: ":", with: "/")
    }
    for prefix in ["https://", "http://", "git://", "ssh://"] {
      if normalized.hasPrefix(prefix) {
        normalized = String(normalized.dropFirst(prefix.count))
        break
      }
    }
    if normalized.hasPrefix("www.") {
      normalized = String(normalized.dropFirst(4))
    }
    if let atIndex = normalized.firstIndex(of: "@"),
       let slashIndex = normalized.firstIndex(of: "/"),
       atIndex < slashIndex {
      normalized = String(normalized[normalized.index(after: atIndex)...])
    }
    return normalized.lowercased()
  }

  /// Synchronously discover the git remote URL for a repo path.
  public static func discoverNormalizedRemoteURL(for path: String) -> String? {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/git")
    process.arguments = ["remote", "get-url", "origin"]
    process.currentDirectoryURL = URL(fileURLWithPath: path)
    let pipe = Pipe()
    process.standardOutput = pipe
    process.standardError = FileHandle.nullDevice
    do {
      try process.run()
      process.waitUntilExit()
      guard process.terminationStatus == 0 else { return nil }
      let data = pipe.fileHandleForReading.readDataToEndOfFile()
      guard let output = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines),
            !output.isEmpty else { return nil }
      return normalizeGitRemoteURL(output)
    } catch {
      return nil
    }
  }

  // MARK: - Repo Identity Resolution

  /// Resolved repo record containing the database ID and current root path.
  public struct ResolvedRepo: Sendable {
    public let id: String
    public let rootPath: String
  }

  /// Resolve a filesystem path to a repo record in the database.
  ///
  /// Resolution order:
  /// 1. Check if `root_path` matches directly (fast path for local repos).
  /// 2. Discover the git remote URL for the given path, normalize it.
  /// 3. Match by `repo_identifier` (normalized git remote URL).
  /// 4. If found via identifier and `root_path` differs, auto-update `root_path`.
  ///
  /// - Parameter repoPath: Absolute filesystem path to the repository.
  /// - Returns: The resolved repo record, or nil if no matching repo found.
  public func resolveRepo(for repoPath: String) throws -> ResolvedRepo? {
    try openIfNeeded()
    guard let db else { return nil }

    // 1. Try exact root_path match
    let directSql = "SELECT id, root_path FROM repos WHERE root_path = ? LIMIT 1"
    var stmt: OpaquePointer?
    if sqlite3_prepare_v2(db, directSql, -1, &stmt, nil) == SQLITE_OK, let s = stmt {
      defer { sqlite3_finalize(s) }
      bindText(s, 1, repoPath)
      if sqlite3_step(s) == SQLITE_ROW {
        let id = String(cString: sqlite3_column_text(s, 0))
        let rootPath = String(cString: sqlite3_column_text(s, 1))
        return ResolvedRepo(id: id, rootPath: rootPath)
      }
    }

    // 2. Discover git remote URL for this path
    let normalizedURL: String?
    if let cached = remoteURLCache[repoPath] {
      normalizedURL = cached
    } else {
      normalizedURL = Self.discoverNormalizedRemoteURL(for: repoPath)
      remoteURLCache[repoPath] = normalizedURL
    }

    guard let identifier = normalizedURL else { return nil }

    // 3. Match by repo_identifier
    let identSql = "SELECT id, root_path FROM repos WHERE repo_identifier = ? LIMIT 1"
    stmt = nil
    if sqlite3_prepare_v2(db, identSql, -1, &stmt, nil) == SQLITE_OK, let s = stmt {
      defer { sqlite3_finalize(s) }
      bindText(s, 1, identifier)
      if sqlite3_step(s) == SQLITE_ROW {
        let repoId = String(cString: sqlite3_column_text(s, 0))
        let storedPath = String(cString: sqlite3_column_text(s, 1))

        // 4. Auto-update root_path if it differs (repo moved or synced from another machine)
        if storedPath != repoPath {
          let escapedPath = repoPath.replacingOccurrences(of: "'", with: "''")
          try? exec("UPDATE repos SET root_path = '\(escapedPath)' WHERE id = '\(repoId)'")
          print("[RAG] Auto-remapped repo \(identifier): \(storedPath) → \(repoPath)")
        }
        return ResolvedRepo(id: repoId, rootPath: repoPath)
      }
    }

    return nil
  }

  /// Resolve a filesystem path to a repo ID.
  ///
  /// Uses `resolveRepo(for:)` to find an existing repo. If not found, falls back
  /// to the legacy behavior of deriving the ID from `stableId(for: path)`.
  ///
  /// - Parameter repoPath: Absolute filesystem path to the repository.
  /// - Returns: The repo ID (either resolved from DB or derived from path).
  public func resolveRepoId(for repoPath: String) throws -> String {
    if let resolved = try resolveRepo(for: repoPath) {
      return resolved.id
    }
    return VectorMath.stableId(for: repoPath)
  }

  // MARK: - Repo Path Remapping

  /// Remap a repository's stored path and ID across all tables.
  ///
  /// Used when importing artifact bundles from machines with different directory layouts.
  /// Updates the repo ID (SHA-256 of path) and root_path in all related tables.
  ///
  /// - Note: **Deprecated.** The `resolveRepo(for:)` method now transparently resolves
  ///   repos via `repo_identifier` (normalized git remote URL) and auto-updates `root_path`
  ///   on mismatch. Manual remapping should no longer be necessary.
  @available(*, deprecated, message: "Use resolveRepo(for:) instead — repos are now resolved by repo_identifier automatically.")
  public func remapRepoPath(oldId: String, newPath: String) throws {
    try openIfNeeded()
    try ensureSchema()

    let newId = VectorMath.stableId(for: newPath)
    let escapedPath = newPath.replacingOccurrences(of: "'", with: "''")

    try exec("UPDATE repos SET root_path = '\(escapedPath)', id = '\(newId)' WHERE id = '\(oldId)'")
    for table in ["files", "symbols", "dependencies", "lessons", "symbol_refs"] {
      try exec("UPDATE \(table) SET repo_id = '\(newId)' WHERE repo_id = '\(oldId)'")
    }
  }
}
