//
//  RAGSearchResult.swift
//  RAGCore
//
//  Search result from vector or text search.
//

import Foundation

/// A single search result from the RAG index.
public struct RAGSearchResult: Sendable {
  public let filePath: String
  public let startLine: Int
  public let endLine: Int
  public let snippet: String

  // Metadata for agent understanding
  public let constructType: String?    // "class", "function", "component", etc.
  public let constructName: String?    // "UserService", "validateForm", etc.
  public let language: String?         // "Swift", "Ruby", "Glimmer TypeScript"
  public let isTest: Bool              // true if in test/spec directory
  public let score: Float?             // relevance score for vector search

  // Facets for filtering/grouping
  public let modulePath: String?       // e.g., "Shared/Services", "Local Packages/Git"
  public let featureTags: [String]     // e.g., ["rag", "indexing"], derived from path/metadata

  // AI analysis results
  public let aiSummary: String?        // AI-generated summary of this chunk
  public let aiTags: [String]          // AI-generated semantic tags

  /// Token count for the original code chunk.
  public let tokenCount: Int?

  /// Number of lines in this chunk.
  public var lineCount: Int { endLine - startLine + 1 }

  public init(
    filePath: String,
    startLine: Int,
    endLine: Int,
    snippet: String,
    constructType: String? = nil,
    constructName: String? = nil,
    language: String? = nil,
    isTest: Bool = false,
    score: Float? = nil,
    modulePath: String? = nil,
    featureTags: [String] = [],
    aiSummary: String? = nil,
    aiTags: [String] = [],
    tokenCount: Int? = nil
  ) {
    self.filePath = filePath
    self.startLine = startLine
    self.endLine = endLine
    self.snippet = snippet
    self.constructType = constructType
    self.constructName = constructName
    self.language = language
    self.isTest = isTest
    self.score = score
    self.modulePath = modulePath
    self.featureTags = featureTags
    self.aiSummary = aiSummary
    self.aiTags = aiTags
    self.tokenCount = tokenCount
  }
}
