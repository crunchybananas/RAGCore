//
//  RAGDependency.swift
//  RAGCore
//
//  Types for code dependency/import graph tracking.
//

import Foundation

/// Type of dependency relationship between code units.
public enum RAGDependencyType: String, Sendable, CaseIterable, Codable {
  case `import` = "import"       // Swift: import, TS/JS: import, Ruby: require
  case require = "require"       // Ruby: require, require_relative
  case include = "include"       // Ruby: include (mixin)
  case extend = "extend"         // Ruby: extend (class methods mixin)
  case inherit = "inherit"       // Class inheritance (< in Ruby, : in Swift)
  case conform = "conform"       // Protocol/interface conformance
  case call = "call"             // Function/method call reference (future)
  case mixin = "mixin"           // Mixin dependency (traits, modules)
  case framework = "framework"   // Framework/package dependency
}

/// Represents a dependency relationship between source and target.
public struct RAGDependency: Sendable {
  public let id: String
  public let repoId: String
  public let sourceFileId: String
  public let sourceSymbolId: String?
  public let targetPath: String           // Resolved path or module name
  public let targetSymbolName: String?    // Optional: specific symbol being imported
  public let targetFileId: String?        // Resolved target file (if in same repo)
  public let dependencyType: RAGDependencyType
  public let rawImport: String            // Original import statement text

  public init(
    id: String = UUID().uuidString,
    repoId: String,
    sourceFileId: String,
    sourceSymbolId: String? = nil,
    targetPath: String,
    targetSymbolName: String? = nil,
    targetFileId: String? = nil,
    dependencyType: RAGDependencyType,
    rawImport: String
  ) {
    self.id = id
    self.repoId = repoId
    self.sourceFileId = sourceFileId
    self.sourceSymbolId = sourceSymbolId
    self.targetPath = targetPath
    self.targetSymbolName = targetSymbolName
    self.targetFileId = targetFileId
    self.dependencyType = dependencyType
    self.rawImport = rawImport
  }
}

/// Result from dependency queries.
public struct RAGDependencyResult: Sendable {
  public let sourceFile: String           // Relative path of source file
  public let targetPath: String           // Module/path being depended on
  public let targetFile: String?          // Resolved target file (if in repo)
  public let dependencyType: RAGDependencyType
  public let rawImport: String

  public init(
    sourceFile: String,
    targetPath: String,
    targetFile: String? = nil,
    dependencyType: RAGDependencyType,
    rawImport: String
  ) {
    self.sourceFile = sourceFile
    self.targetPath = targetPath
    self.targetFile = targetFile
    self.dependencyType = dependencyType
    self.rawImport = rawImport
  }
}

/// Summary of dependencies for a file.
public struct RAGDependencySummary: Sendable {
  public let filePath: String
  public let dependencies: [RAGDependencyResult]    // What this file depends on
  public let dependents: [RAGDependencyResult]      // What depends on this file

  public init(
    filePath: String,
    dependencies: [RAGDependencyResult],
    dependents: [RAGDependencyResult]
  ) {
    self.filePath = filePath
    self.dependencies = dependencies
    self.dependents = dependents
  }
}

/// Lightweight file summary for dependency graph aggregation.
public struct RAGFileSummary: Sendable {
  public let path: String
  public let language: String?
  public let modulePath: String?

  public init(path: String, language: String? = nil, modulePath: String? = nil) {
    self.path = path
    self.language = language
    self.modulePath = modulePath
  }
}
