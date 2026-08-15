//
//  RAGFileScanner.swift
//  RAGCore
//
//  Directory scanner for discovering and loading source files to index.
//

import Foundation

/// Scans directories to discover source files for RAG indexing.
///
/// Supports `.ragignore` files (like `.gitignore`), excludes known
/// non-essential directories and lock files, and detects 50+ file
/// extensions across Swift, TypeScript, Ruby, Python, Rust, Go, and more.
public struct RAGFileScanner: Sendable {

  public var maxFileBytes: Int

  public var excludedDirectories: Set<String>

  /// Files that are always excluded regardless of extension.
  private let excludedFiles: Set<String> = [
    "pnpm-lock.yaml",
    "package-lock.json",
    "yarn.lock",
    "Gemfile.lock",
    "Podfile.lock",
    "Cargo.lock",
    "composer.lock",
    "poetry.lock",
  ]

  /// File patterns to exclude (checked against filename).
  private let excludedPatterns: [String] = [
    ".min.",
    ".bundle.",
    ".chunk.",
    "-bundle.",
    ".packed.",
  ]

  /// The mandatory credential boundary (#11). Always consulted BEFORE any
  /// other filter and before any byte is read; `.ragignore` cannot weaken it.
  /// The only relaxation is the policy's own per-pattern unsafe opt-in.
  public var credentialPolicy: CredentialExclusionPolicy

  public init(
    maxFileBytes: Int = 1_000_000,
    excludedDirectories: Set<String>? = nil,
    credentialPolicy: CredentialExclusionPolicy = .standard
  ) {
    self.maxFileBytes = maxFileBytes
    self.excludedDirectories = excludedDirectories ?? Self.defaultExcludedDirectories
    self.credentialPolicy = credentialPolicy
  }

  public static let defaultExcludedDirectories: Set<String> = [
    ".git",
    ".build",
    ".swiftpm",
    "build",
    "dist",
    "DerivedData",
    "node_modules",
    "coverage",
    "tmp",
    "Carthage",
    ".turbo",
    "__snapshots__",
    "vendor",
  ]

  /// A scan's full outcome: the indexable candidates plus the root-relative
  /// paths the mandatory credential policy refused (#11). Paths only — the
  /// refused files were never read, so there are no contents to leak.
  public struct ScanOutcome: Sendable {
    public let candidates: [RAGFileCandidate]
    public let policyExcludedPaths: [String]
  }

  /// Scan a directory for indexable source files.
  ///
  /// - Parameters:
  ///   - rootURL: Root directory to scan.
  ///   - excludingRoots: Absolute paths to skip entirely (e.g. sub-repo roots).
  /// - Returns: Array of file candidates with their paths, sizes, and languages.
  public func scan(rootURL: URL, excludingRoots: [String] = []) -> [RAGFileCandidate] {
    scan(rootURL: rootURL, excludingRoots: excludingRoots, cancellationCheck: {}).candidates
  }

  /// Scan and also report what the credential policy refused (#11).
  public func scanWithOutcome(rootURL: URL, excludingRoots: [String] = []) -> ScanOutcome {
    scan(rootURL: rootURL, excludingRoots: excludingRoots, cancellationCheck: {})
  }

  /// Scan while cooperatively honoring cancellation between filesystem entries.
  ///
  /// Kept internal because the public non-throwing API predates cancellation.
  /// Indexing uses this path so a cancelled task does not finish walking a large
  /// repository before it can stop.
  internal func scanCancellable(rootURL: URL, excludingRoots: [String] = []) throws -> ScanOutcome {
    try scan(
      rootURL: rootURL,
      excludingRoots: excludingRoots,
      cancellationCheck: { try Task.checkCancellation() }
    )
  }

  private func scan(
    rootURL: URL,
    excludingRoots: [String],
    cancellationCheck: () throws -> Void
  ) rethrows -> ScanOutcome {
    try cancellationCheck()
    guard let enumerator = FileManager.default.enumerator(
      at: rootURL,
      includingPropertiesForKeys: [.isDirectoryKey, .fileSizeKey],
      options: [.skipsHiddenFiles, .skipsPackageDescendants]
    ) else {
      return ScanOutcome(candidates: [], policyExcludedPaths: [])
    }

    let ignorePatterns = Self.loadIgnorePatterns(rootURL: rootURL)
    var results: [RAGFileCandidate] = []
    var policyExcluded: [String] = []

    for case let fileURL as URL in enumerator {
      try cancellationCheck()

      // The credential boundary runs FIRST, before every other filter, so no
      // later rule (and no .ragignore content) influences it. Checked on the
      // entry's own name/path AND, for symlinks, on the resolved target's
      // name — a link named notes.txt pointing at id_rsa is still a
      // credential read (#11).
      let isDirectoryEntry = (try? fileURL.resourceValues(
        forKeys: [.isDirectoryKey]
      ).isDirectory) == true
      if !isDirectoryEntry {
        let relative = Self.rootRelativePath(of: fileURL.path, under: rootURL)
          ?? fileURL.lastPathComponent
        var refused = credentialPolicy.excludes(
          relativePath: relative, fileName: fileURL.lastPathComponent)
        if !refused,
           let destination = try? FileManager.default.destinationOfSymbolicLink(atPath: fileURL.path) {
          let targetName = (destination as NSString).lastPathComponent
          refused = credentialPolicy.excludes(relativePath: destination, fileName: targetName)
        }
        if refused {
          policyExcluded.append(relative)
          continue
        }
      }

      if shouldSkip(url: fileURL, rootURL: rootURL, ignorePatterns: ignorePatterns, excludedRoots: excludingRoots) {
        // `skipDescendants()` is only valid for a directory. Calling it after
        // an ignored file (for example `poetry.lock`) makes Foundation skip
        // the file's later siblings too, which can silently prune an entire
        // source directory from the scan.
        if isDirectoryEntry {
          enumerator.skipDescendants()
        }
        continue
      }

      guard isTextFile(url: fileURL) else { continue }

      let size = fileSize(for: fileURL)
      let byteCount = min(max(0, size), maxFileBytes)
      guard byteCount > 0 else { continue }
      results.append(
        RAGFileCandidate(
          path: fileURL.path,
          byteCount: byteCount,
          language: languageFor(url: fileURL)
        )
      )
    }

    return ScanOutcome(candidates: results, policyExcludedPaths: policyExcluded)
  }

  /// Load a file candidate into memory.
  public func loadFile(candidate: RAGFileCandidate) -> RAGScannedFile? {
    let url = URL(fileURLWithPath: candidate.path)
    // Defense in depth: the scan gate is the boundary, but this is the one
    // place in the package that reads repository bytes, so it refuses
    // credential-shaped paths on its own too — a future caller that builds
    // candidates without scanning inherits the guarantee (#11).
    guard !credentialPolicy.excludes(
      relativePath: candidate.path, fileName: url.lastPathComponent
    ) else { return nil }
    guard let data = try? Data(contentsOf: url, options: [.mappedIfSafe]) else {
      return nil
    }

    let slice = data.prefix(candidate.byteCount)
    guard let text = String(data: slice, encoding: .utf8) else { return nil }
    let lineCount = text.split(separator: "\n", omittingEmptySubsequences: false).count

    return RAGScannedFile(
      path: candidate.path,
      text: text,
      lineCount: lineCount,
      byteCount: candidate.byteCount,
      language: candidate.language
    )
  }

  // MARK: - Private Helpers

  private func shouldSkip(url: URL, rootURL: URL, ignorePatterns: [String], excludedRoots: [String]) -> Bool {
    let lastComponent = url.lastPathComponent
    let path = url.path
    for root in excludedRoots {
      if path == root || path.hasPrefix(root + "/") {
        return true
      }
    }
    if excludedDirectories.contains(lastComponent) {
      return true
    }
    if excludedFiles.contains(lastComponent) {
      return true
    }
    let lowercasedName = lastComponent.lowercased()
    for pattern in excludedPatterns {
      if lowercasedName.contains(pattern) {
        return true
      }
    }
    if Self.matchesIgnore(url: url, rootURL: rootURL, patterns: ignorePatterns) {
      return true
    }
    return false
  }

  /// Root-relative form of `path` under `rootURL`, robust to the /var vs
  /// /private/var standardization mismatch: Foundation's enumerator can
  /// return standardized URLs for a non-standardized root (temporary
  /// directories hit this every time), and a failed prefix match silently
  /// degraded relative paths to bare file names — which weakened every
  /// path-anchored ignore pattern for such roots.
  internal static func rootRelativePath(of path: String, under rootURL: URL) -> String? {
    var roots = Set<String>()
    for base in [rootURL.path, rootURL.standardizedFileURL.path, rootURL.resolvingSymlinksInPath().path] {
      // Foundation is asymmetric about the /private alias: the enumerator
      // returns /private/var/... entries while resolvingSymlinksInPath on the
      // root STRIPS /private (a documented oddity), so neither side alone
      // ever matches for temporary directories. Track both spellings.
      let slashed = base.hasSuffix("/") ? base : base + "/"
      roots.insert(slashed)
      if slashed.hasPrefix("/private/") {
        roots.insert(String(slashed.dropFirst("/private".count)))
      } else {
        roots.insert("/private" + slashed)
      }
    }
    for root in roots where path.hasPrefix(root) {
      return String(path.dropFirst(root.count))
    }
    return nil
  }

  /// Parse the scan root's `.ragignore` AND root `.gitignore` into raw
  /// patterns. Shared with the workspace/sub-package detectors so directory
  /// discovery honors the same ignore files as file scanning
  /// (crunchybananas/RAGCore#4, #11).
  ///
  /// Precedence is purely additive and documented (#11): the mandatory
  /// credential policy is checked before any of this and cannot be affected
  /// by it; `.ragignore` and root `.gitignore` patterns then both EXCLUDE.
  /// Negated (`!`) lines are dropped from both files rather than emulated:
  /// the matcher has no re-include machinery, and silently treating `!foo`
  /// as an exclusion of the literal name `!foo` (the old behavior for
  /// `.ragignore`) is stricter than gitignore in the safe direction, but
  /// re-including is the unsafe direction and stays unsupported. Nested
  /// `.gitignore` files are not read — root patterns only; a repository
  /// needing deeper policy uses `.ragignore` at the root.
  internal static func loadIgnorePatterns(rootURL: URL) -> [String] {
    let ragPatterns = patternLines(of: rootURL.appendingPathComponent(".ragignore"))
    let gitPatterns = patternLines(of: rootURL.appendingPathComponent(".gitignore"))
    return ragPatterns + gitPatterns
  }

  private static func patternLines(of fileURL: URL) -> [String] {
    guard let contents = try? String(contentsOf: fileURL, encoding: .utf8) else {
      return []
    }
    return contents
      .split(whereSeparator: { $0 == "\n" || $0 == "\r" })
      .map { $0.trimmingCharacters(in: .whitespaces) }
      .filter { !$0.isEmpty && !$0.hasPrefix("#") && !$0.hasPrefix("!") }
  }

  /// Match a path against `.ragignore` patterns.
  ///
  /// - Parameter isDirectory: When true, a trailing-slash directory pattern
  ///   (`build-ios-check/`) also matches the directory itself, not only paths
  ///   under it — file scanning only needs the children to match (the walk
  ///   skips them one by one), but directory discovery must prune the
  ///   directory node.
  internal static func matchesIgnore(url: URL, rootURL: URL, patterns: [String], isDirectory: Bool = false) -> Bool {
    guard !patterns.isEmpty else { return false }
    let path = url.path
    let relative = Self.rootRelativePath(of: path, under: rootURL) ?? path
    let fileName = url.lastPathComponent

    for pattern in patterns {
      if fnmatch(pattern, relative, 0) == 0 { return true }
      if fnmatch(pattern, fileName, 0) == 0 { return true }
      if pattern.hasSuffix("/") {
        if relative.hasPrefix(pattern) { return true }
        if isDirectory {
          let stripped = String(pattern.dropLast())
          if fnmatch(stripped, relative, 0) == 0 { return true }
          if fnmatch(stripped, fileName, 0) == 0 { return true }
          // gitignore semantics: `**/foo/` matches foo at ANY depth,
          // including the root — but the stripped pattern's literal `/`
          // after `**` can never match a root-level name, so also try with
          // the `**/` prefix removed.
          if stripped.hasPrefix("**/") {
            let anchored = String(stripped.dropFirst(3))
            if fnmatch(anchored, relative, 0) == 0 { return true }
            if fnmatch(anchored, fileName, 0) == 0 { return true }
          }
        }
      }
    }
    return false
  }

  private func isTextFile(url: URL) -> Bool {
    let ext = url.pathExtension.lowercased()
    if ext.isEmpty { return false }
    return Self.supportedExtensions.contains(ext)
  }

  private func fileSize(for url: URL) -> Int {
    if let values = try? url.resourceValues(forKeys: [.fileSizeKey]),
       let size = values.fileSize {
      return size
    }
    if let attrs = try? FileManager.default.attributesOfItem(atPath: url.path),
       let fileSize = attrs[.size] as? NSNumber {
      return fileSize.intValue
    }
    return 0
  }

  // MARK: - Language Detection

  /// Detect language from file extension.
  public func languageFor(url: URL) -> String {
    switch url.pathExtension.lowercased() {
    case "swift": return "Swift"
    case "js", "jsx", "mjs", "cjs": return "JavaScript"
    case "ts", "tsx", "mts", "cts": return "TypeScript"
    case "gts": return "Glimmer TypeScript"
    case "gjs": return "Glimmer JavaScript"
    case "hbs": return "Handlebars"
    case "vue": return "Vue"
    case "svelte": return "Svelte"
    case "astro": return "Astro"
    case "rb", "rake", "gemspec": return "Ruby"
    case "erb": return "ERB"
    case "py", "pyi", "pyx": return "Python"
    case "rs": return "Rust"
    case "go": return "Go"
    case "c", "h": return "C"
    case "cpp", "hpp", "cc", "cxx": return "C++"
    case "java": return "Java"
    case "kt", "kts": return "Kotlin"
    case "scala": return "Scala"
    case "groovy", "gradle": return "Groovy"
    case "md", "mdx": return "Markdown"
    case "txt": return "Text"
    case "rst": return "reStructuredText"
    case "adoc": return "AsciiDoc"
    case "json", "jsonc", "json5": return "JSON"
    case "yml", "yaml": return "YAML"
    case "toml": return "TOML"
    case "xml", "plist": return "XML"
    case "css", "scss", "sass", "less", "styl": return "CSS"
    case "html", "htm": return "HTML"
    case "ejs", "njk", "liquid": return "Template"
    case "sh", "bash", "zsh", "fish": return "Shell"
    case "ps1": return "PowerShell"
    case "bat", "cmd": return "Batch"
    case "sql": return "SQL"
    case "graphql", "gql": return "GraphQL"
    case "prisma": return "Prisma"
    case "dockerfile": return "Dockerfile"
    case "tf", "hcl": return "Terraform"
    case "proto": return "Protocol Buffers"
    case "cfg", "ini", "conf", "env": return "Config"
    default: return url.pathExtension.uppercased()
    }
  }

  /// Comprehensive list of code and config file extensions for RAG indexing.
  public static let supportedExtensions: Set<String> = {
    var extensions = Set<String>()
    extensions.formUnion(["swift"])
    extensions.formUnion(["js", "ts", "tsx", "jsx", "mjs", "cjs", "mts", "cts"])
    extensions.formUnion(["gts", "gjs", "hbs"])
    extensions.formUnion(["vue", "svelte", "astro"])
    extensions.formUnion(["rb", "rake", "gemspec", "erb"])
    extensions.formUnion(["py", "pyi", "pyx"])
    extensions.formUnion(["rs", "go", "c", "h", "cpp", "hpp", "cc", "cxx"])
    extensions.formUnion(["java", "kt", "kts", "scala", "groovy", "gradle"])
    extensions.formUnion(["md", "mdx", "txt", "rst", "adoc"])
    extensions.formUnion(["json", "jsonc", "json5", "yml", "yaml", "toml", "xml", "plist"])
    extensions.formUnion(["css", "scss", "sass", "less", "styl"])
    extensions.formUnion(["html", "htm", "ejs", "njk", "liquid"])
    extensions.formUnion(["sh", "bash", "zsh", "fish", "ps1", "bat", "cmd"])
    extensions.formUnion(["sql", "graphql", "gql", "prisma"])
    extensions.formUnion(["dockerfile", "tf", "hcl", "proto"])
    extensions.formUnion(["cfg", "ini", "conf", "env"])
    return extensions
  }()
}

/// Lightweight struct for decoding chunk metadata to extract facets.
public struct ChunkMetadataForFacets: Decodable, Sendable {
  public let frameworks: [String]?
  public let usesEmberConcurrency: Bool?
  public let hasTemplate: Bool?
}
