//
//  RAGQueryHint.swift
//  RAGCore
//
//  Query hint tracking for search analytics and autocomplete.
//

import Foundation

/// A recorded query hint for search analytics and autocomplete.
public struct RAGQueryHint: Sendable {
  public let id: String
  public let query: String
  public let resultCount: Int
  public let searchMode: String
  public let createdAt: String

  public init(
    id: String,
    query: String,
    resultCount: Int,
    searchMode: String,
    createdAt: String
  ) {
    self.id = id
    self.query = query
    self.resultCount = resultCount
    self.searchMode = searchMode
    self.createdAt = createdAt
  }
}
