//
//  HybridChunker.swift
//  RAGCore
//
//  Hybrid chunker that uses AST-aware chunking for supported languages
//  and falls back to line-based chunking for others.
//

import ASTChunker
import Foundation

/// Hybrid chunker that uses AST-aware chunking for supported languages
/// (Swift, Ruby, TypeScript/JavaScript/GTS/GJS) and falls back to
/// line-based chunking for others.
///
/// - Swift uses subprocess isolation (ast-chunker-cli) to prevent stack overflow crashes.
/// - Ruby uses tree-sitter via the `RubyChunker`.
/// - TypeScript/JavaScript/GTS/GJS use JavaScriptCore-based chunker.
public struct HybridChunker: Sendable {
  private let lineChunker = RAGLineChunker()
  private let rubyChunker: RubyChunker?
  private let jsChunker: JSCoreTypeScriptChunker

  /// Path to ast-chunker-cli for Swift subprocess chunking.
  private let astChunkerCLIPath: String?

  /// Subprocess timeout for Swift AST parsing.
  private let swiftSubprocessTimeout: TimeInterval = 5.0

  /// Max file size for Swift subprocess (very large files still timeout).
  private let swiftSubprocessMaxBytes = 500_000  // 500KB

  /// Max file size for tree-sitter parsing.
  private let treeSitterMaxBytes = 500_000  // 500KB

  /// Max file size for JSCore parsing.
  private let jsMaxBytes = 500_000  // 500KB

  /// A signature capturing available chunking capabilities.
  /// Including this in file hashes ensures files are re-chunked when
  /// chunker availability changes (e.g., JSCore becomes available).
  /// Bump the base version when chunking logic changes materially.
  public var chunkingSignature: String {
    var sig = "chunk-v3"
    if astChunkerCLIPath != nil { sig += "+swift" }
    if rubyChunker != nil { sig += "+ruby" }
    if jsChunker.isAvailable { sig += "+jscore" }
    return sig
  }

  /// Languages that have AST chunker support.
  public var astSupportedLanguages: Set<String> {
    var languages: Set<String> = []
    if astChunkerCLIPath != nil {
      languages.insert("Swift")
    }
    if rubyChunker != nil {
      languages.insert("Ruby")
    }
    if jsChunker.isAvailable {
      languages.insert("TypeScript")
      languages.insert("JavaScript")
      languages.insert("Glimmer TypeScript")
      languages.insert("Glimmer JavaScript")
    }
    return languages
  }

  /// Create a hybrid chunker.
  ///
  /// - Parameters:
  ///   - astChunkerCLIPath: Explicit path to ast-chunker-cli. If nil, auto-discovers.
  ///   - searchPaths: Additional paths to search for ast-chunker-cli binary.
  public init(astChunkerCLIPath: String? = nil, searchPaths: [String] = []) {
    let ruby = RubyChunker()
    self.rubyChunker = ruby.isAvailable ? ruby : nil
    self.astChunkerCLIPath = astChunkerCLIPath ?? Self.findASTChunkerCLI(searchPaths: searchPaths)
    self.jsChunker = JSCoreTypeScriptChunker.shared

    print("[HybridChunker] Ruby chunker available: \(rubyChunker != nil)")
    print("[HybridChunker] Swift CLI available: \(self.astChunkerCLIPath != nil) at \(self.astChunkerCLIPath ?? "N/A")")
    print("[HybridChunker] JSCore TS/JS/GTS/GJS chunker available: \(jsChunker.isAvailable)")
    print("[HybridChunker] AST supported languages: \(astSupportedLanguages)")
    print("[HybridChunker] Chunking signature: \(chunkingSignature)")
  }

  /// Chunk with full error tracking and health-aware fallback.
  public func chunkSafe(
    text: String,
    language: String,
    filePath: String,
    fileHash: String,
    healthTracker: ChunkingHealthTracker
  ) -> ChunkingResult {
    if healthTracker.shouldSkipAST(for: filePath, hash: fileHash) {
      print("[HybridChunker] Skipping AST for \(filePath) due to previous failure")
      return ChunkingResult(
        chunks: lineChunker.chunk(text: text),
        usedAST: false,
        failureType: nil,
        failureMessage: "Skipped due to previous failure"
      )
    }

    if astSupportedLanguages.contains(language) {
      return chunkWithASTSafe(text: text, language: language, filePath: filePath)
    }

    return ChunkingResult(
      chunks: lineChunker.chunk(text: text),
      usedAST: false,
      failureType: nil,
      failureMessage: nil
    )
  }

  /// Legacy method for backward compatibility.
  public func chunk(text: String, language: String) -> [RAGChunk] {
    if astSupportedLanguages.contains(language) {
      let chunks = chunkWithAST(text: text, language: language)
      print("[RAG] AST chunking \(language): \(chunks.count) chunks")
      return chunks
    }
    return lineChunker.chunk(text: text)
  }

  // MARK: - Internal

  private func chunkWithASTSafe(text: String, language: String, filePath: String) -> ChunkingResult {
    let byteCount = text.utf8.count

    switch language {
    case "Swift":
      if byteCount > swiftSubprocessMaxBytes {
        return ChunkingResult(
          chunks: lineChunker.chunk(text: text),
          usedAST: false,
          failureType: .timeout,
          failureMessage: "File too large: \(byteCount) bytes"
        )
      }
      return chunkSwiftWithSubprocess(text: text, filePath: filePath)
    case "Ruby":
      if byteCount > treeSitterMaxBytes {
        return ChunkingResult(
          chunks: lineChunker.chunk(text: text),
          usedAST: false,
          failureType: .timeout,
          failureMessage: "File too large: \(byteCount) bytes"
        )
      }
    case "TypeScript", "JavaScript", "Glimmer TypeScript", "Glimmer JavaScript":
      if byteCount > jsMaxBytes {
        return ChunkingResult(
          chunks: lineChunker.chunk(text: text),
          usedAST: false,
          failureType: .timeout,
          failureMessage: "File too large: \(byteCount) bytes"
        )
      }
      return chunkJSWithJSCore(text: text, language: language, filePath: filePath)
    default:
      break
    }

    let astChunks = chunkWithAST(text: text, language: language)
    if astChunks.isEmpty {
      return ChunkingResult(
        chunks: lineChunker.chunk(text: text),
        usedAST: false,
        failureType: .parseError,
        failureMessage: "AST returned empty"
      )
    }

    return ChunkingResult(
      chunks: astChunks,
      usedAST: true,
      failureType: nil,
      failureMessage: nil
    )
  }

  // MARK: - Swift Subprocess Chunking

  private struct CLIChunk: Codable {
    let startLine: Int
    let endLine: Int
    let text: String
    let constructType: String
    let constructName: String?
    let tokenCount: Int
    let metadata: String?
  }

  private func chunkSwiftWithSubprocess(text: String, filePath: String) -> ChunkingResult {
    guard let cliPath = astChunkerCLIPath else {
      return ChunkingResult(
        chunks: lineChunker.chunk(text: text),
        usedAST: false,
        failureType: nil,
        failureMessage: "CLI not available"
      )
    }

    let tempDir = FileManager.default.temporaryDirectory
    let tempFile = tempDir.appendingPathComponent("swift_\(UUID().uuidString).swift")

    do {
      try text.write(to: tempFile, atomically: true, encoding: .utf8)
    } catch {
      return ChunkingResult(
        chunks: lineChunker.chunk(text: text),
        usedAST: false,
        failureType: .parseError,
        failureMessage: "Failed to write temp file"
      )
    }

    defer {
      try? FileManager.default.removeItem(at: tempFile)
    }

    let process = Process()
    let pipe = Pipe()
    let errorPipe = Pipe()

    process.executableURL = URL(fileURLWithPath: cliPath)
    process.arguments = ["--json", tempFile.path]
    process.standardOutput = pipe
    process.standardError = errorPipe

    final class DataBox: @unchecked Sendable {
      var data = Data()
    }
    let outputBox = DataBox()
    let errorBox = DataBox()
    let group = DispatchGroup()

    group.enter()
    DispatchQueue.global(qos: .userInitiated).async {
      outputBox.data = pipe.fileHandleForReading.readDataToEndOfFile()
      group.leave()
    }

    group.enter()
    DispatchQueue.global(qos: .userInitiated).async {
      errorBox.data = errorPipe.fileHandleForReading.readDataToEndOfFile()
      group.leave()
    }

    do {
      try process.run()

      let result = group.wait(timeout: .now() + swiftSubprocessTimeout)
      if result == .timedOut {
        process.terminate()
        let fileName = (filePath as NSString).lastPathComponent
        print("[HybridChunker] Swift CLI timeout for \(fileName)")
        return ChunkingResult(
          chunks: lineChunker.chunk(text: text),
          usedAST: false,
          failureType: .timeout,
          failureMessage: "Subprocess timeout"
        )
      }

      process.waitUntilExit()

      let outputData = outputBox.data
      let errorData = errorBox.data

      if process.terminationStatus != 0 {
        let errorMsg = String(data: errorData, encoding: .utf8) ?? "Unknown error"
        let fileName = (filePath as NSString).lastPathComponent
        print("[HybridChunker] Swift CLI failed for \(fileName): exit \(process.terminationStatus), \(errorMsg)")
        return ChunkingResult(
          chunks: lineChunker.chunk(text: text),
          usedAST: false,
          failureType: .crash,
          failureMessage: "CLI exit \(process.terminationStatus)"
        )
      }

      let decoder = JSONDecoder()
      let cliChunks = try decoder.decode([CLIChunk].self, from: outputData)

      let fileName = (filePath as NSString).lastPathComponent
      let chunksWithMeta = cliChunks.filter { $0.metadata != nil }.count
      print("[HybridChunker] Swift CLI for \(fileName): \(cliChunks.count) chunks, \(chunksWithMeta) with metadata")

      if cliChunks.isEmpty {
        return ChunkingResult(
          chunks: lineChunker.chunk(text: text),
          usedAST: false,
          failureType: .parseError,
          failureMessage: "CLI returned empty chunks"
        )
      }

      let chunks = cliChunks.map { cli in
        RAGChunk(
          startLine: cli.startLine,
          endLine: cli.endLine,
          text: cli.text,
          tokenCount: cli.tokenCount,
          constructType: cli.constructType,
          constructName: cli.constructName,
          metadata: cli.metadata
        )
      }

      return ChunkingResult(
        chunks: chunks,
        usedAST: true,
        failureType: nil,
        failureMessage: nil
      )

    } catch {
      let fileName = (filePath as NSString).lastPathComponent
      print("[HybridChunker] Swift CLI error for \(fileName): \(error)")
      return ChunkingResult(
        chunks: lineChunker.chunk(text: text),
        usedAST: false,
        failureType: .parseError,
        failureMessage: error.localizedDescription
      )
    }
  }

  // MARK: - TypeScript/JavaScript JSCore Chunking

  private func chunkJSWithJSCore(text: String, language: String, filePath: String) -> ChunkingResult {
    let fileName = (filePath as NSString).lastPathComponent
    let ext = mapLanguageToExtension(language, filePath: filePath)
    let astChunks = jsChunker.chunk(source: text, language: ext)

    if astChunks.isEmpty {
      return ChunkingResult(
        chunks: lineChunker.chunk(text: text),
        usedAST: false,
        failureType: .parseError,
        failureMessage: "JSCore returned empty"
      )
    }

    let allFileType = astChunks.allSatisfy { $0.constructType == .file }
    if allFileType && astChunks.count > 1 {
      print("[HybridChunker] JSCore fell back to line chunking for \(fileName) (\(astChunks.count) file chunks)")
    }

    let chunks = astChunks.map { chunk in
      RAGChunk(
        startLine: chunk.startLine,
        endLine: chunk.endLine,
        text: chunk.text,
        tokenCount: chunk.estimatedTokenCount,
        constructType: chunk.constructType.rawValue,
        constructName: chunk.constructName,
        metadata: chunk.metadata.toJSON()
      )
    }

    print("[HybridChunker] JSCore: \(chunks.count) chunks for \(fileName)")

    return ChunkingResult(
      chunks: chunks,
      usedAST: true,
      failureType: nil,
      failureMessage: nil
    )
  }

  private func mapLanguageToExtension(_ language: String, filePath: String) -> String {
    let ext = (filePath as NSString).pathExtension.lowercased()
    if !ext.isEmpty && ["ts", "tsx", "js", "jsx", "mts", "cts", "mjs", "cjs", "gts", "gjs"].contains(ext) {
      return ext
    }
    switch language {
    case "TypeScript": return "ts"
    case "JavaScript": return "js"
    default: return "ts"
    }
  }

  private func chunkWithAST(text: String, language: String) -> [RAGChunk] {
    let astChunks: [ASTChunk]

    switch language {
    case "Swift":
      print("[HybridChunker] Warning: Swift should use subprocess path")
      return lineChunker.chunk(text: text)
    case "Ruby":
      if let rubyChunker = rubyChunker {
        astChunks = rubyChunker.chunk(source: text)
      } else {
        return lineChunker.chunk(text: text)
      }
    case "Glimmer TypeScript", "Glimmer JavaScript":
      let lang = language == "Glimmer TypeScript" ? "gts" : "gjs"
      astChunks = jsChunker.chunk(source: text, language: lang)
    case "TypeScript", "JavaScript":
      astChunks = jsChunker.chunk(source: text, language: language == "TypeScript" ? "ts" : "js")
    default:
      return lineChunker.chunk(text: text)
    }

    return astChunks.map { astChunk in
      RAGChunk(
        startLine: astChunk.startLine,
        endLine: astChunk.endLine,
        text: astChunk.text,
        tokenCount: astChunk.estimatedTokenCount,
        constructType: astChunk.constructType.rawValue,
        constructName: astChunk.constructName,
        metadata: astChunk.metadata.toJSON()
      )
    }
  }

  // MARK: - CLI Discovery

  /// Find the ast-chunker-cli binary.
  ///
  /// Search order:
  /// 1. App bundle executable directory
  /// 2. App bundle Frameworks directory
  /// 3. Additional `searchPaths` provided by the caller
  /// 4. Source-file relative paths (development builds)
  private static func findASTChunkerCLI(searchPaths: [String] = []) -> String? {
    // Check in app bundle first
    if let bundlePath = Bundle.main.executableURL?.deletingLastPathComponent()
        .appendingPathComponent("ast-chunker-cli").path,
       FileManager.default.fileExists(atPath: bundlePath) {
      return bundlePath
    }

    // Check Frameworks directory
    if let frameworksPath = Bundle.main.privateFrameworksPath {
      let cliPath = (frameworksPath as NSString).appendingPathComponent("ast-chunker-cli")
      if FileManager.default.fileExists(atPath: cliPath) {
        return cliPath
      }
    }

    // Check caller-provided search paths
    for searchPath in searchPaths {
      let cliPath = (searchPath as NSString).appendingPathComponent("ast-chunker-cli")
      if FileManager.default.fileExists(atPath: cliPath) {
        return cliPath
      }
    }

    // Development: resolve relative to this source file's location
    let repoRoot = findRepoRootFromSourceFile()
    if let root = repoRoot {
      let toolsBinPath = "\(root)/Tools/Binaries/ast-chunker-cli"
      if FileManager.default.fileExists(atPath: toolsBinPath) {
        return toolsBinPath
      }

      for config in ["release", "debug"] {
        let cliPath = "\(root)/Local Packages/ASTChunker/.build/\(config)/ast-chunker-cli"
        if FileManager.default.fileExists(atPath: cliPath) {
          return cliPath
        }
      }
    }

    return nil
  }

  private static func findRepoRootFromSourceFile() -> String? {
    var url = URL(fileURLWithPath: #filePath)
    // Walk up: HybridChunker.swift -> Chunking/ -> RAGCore/ -> Sources/ -> RAGCore/ -> Local Packages/ -> <repo root>
    for _ in 0..<6 {
      url = url.deletingLastPathComponent()
    }
    let root = url.path
    if FileManager.default.fileExists(atPath: (root as NSString).appendingPathComponent("Local Packages")) {
      return root
    }
    return nil
  }
}
