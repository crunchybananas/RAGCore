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

  public init(
    maxFileBytes: Int = 1_000_000,
    excludedDirectories: Set<String>? = nil
  ) {
    self.maxFileBytes = maxFileBytes
    self.excludedDirectories = excludedDirectories ?? Self.defaultExcludedDirectories
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

  /// Scan a directory for indexable source files.
  ///
  /// - Parameters:
  ///   - rootURL: Root directory to scan.
  ///   - excludingRoots: Absolute paths to skip entirely (e.g. sub-repo roots).
  /// - Returns: Array of file candidates with their paths, sizes, and languages.
  public func scan(rootURL: URL, excludingRoots: [String] = []) -> [RAGFileCandidate] {
    guard let enumerator = FileManager.default.enumerator(
      at: rootURL,
      includingPropertiesForKeys: [.isDirectoryKey, .fileSizeKey],
      options: [.skipsHiddenFiles, .skipsPackageDescendants]
    ) else {
      return []
    }

    let ignorePatterns = loadIgnorePatterns(rootURL: rootURL)
    var results: [RAGFileCandidate] = []

    for case let fileURL as URL in enumerator {
      if shouldSkip(url: fileURL, rootURL: rootURL, ignorePatterns: ignorePatterns, excludedRoots: excludingRoots) {
        enumerator.skipDescendants()
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

    return results
  }

  /// Load a file candidate into memory.
  public func loadFile(candidate: RAGFileCandidate) -> RAGScannedFile? {
    let url = URL(fileURLWithPath: candidate.path)
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
    if matchesIgnore(url: url, rootURL: rootURL, patterns: ignorePatterns) {
      return true
    }
    return false
  }

  private func loadIgnorePatterns(rootURL: URL) -> [String] {
    let ignoreURL = rootURL.appendingPathComponent(".ragignore")
    guard let contents = try? String(contentsOf: ignoreURL, encoding: .utf8) else {
      return []
    }
    return contents
      .split(whereSeparator: { $0 == "\n" || $0 == "\r" })
      .map { $0.trimmingCharacters(in: .whitespaces) }
      .filter { !$0.isEmpty && !$0.hasPrefix("#") }
  }

  private func matchesIgnore(url: URL, rootURL: URL, patterns: [String]) -> Bool {
    guard !patterns.isEmpty else { return false }
    let path = url.path
    let rootPath = rootURL.path.hasSuffix("/") ? rootURL.path : rootURL.path + "/"
    let relative = path.hasPrefix(rootPath) ? String(path.dropFirst(rootPath.count)) : path
    let fileName = url.lastPathComponent

    for pattern in patterns {
      if fnmatch(pattern, relative, 0) == 0 { return true }
      if fnmatch(pattern, fileName, 0) == 0 { return true }
      if pattern.hasSuffix("/") && relative.hasPrefix(pattern) { return true }
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
