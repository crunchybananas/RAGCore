//
//  RAGLesson.swift
//  RAGCore
//
//  Learned pattern from agent mistakes/fixes that improves future runs.
//

import Foundation

/// A learned pattern from agent mistakes/fixes that improves future runs.
public struct RAGLesson: Sendable, Identifiable {
  public let id: String
  public let repoId: String
  public let filePattern: String?        // e.g., "*.gts", "app/models/*.rb"
  public let errorSignature: String?     // Normalized error pattern for matching
  public let fixDescription: String      // Human-readable description of the fix
  public let fixCode: String?            // Actual code snippet that fixed the issue
  public let confidence: Double          // 0.0-1.0, increases with successful applications
  public let isActive: Bool              // Can be disabled without deletion
  public let createdAt: String           // ISO 8601 date string
  public let updatedAt: String?          // ISO 8601 date string
  public let applyCount: Int             // How many times this lesson was applied
  public let successCount: Int           // How many times applying succeeded
  public let source: String              // "auto" (detected), "manual" (user added), "imported"

  public init(
    id: String = UUID().uuidString,
    repoId: String,
    errorSignature: String? = nil,
    filePattern: String? = nil,
    fixDescription: String,
    fixCode: String? = nil,
    source: String = "manual",
    confidence: Double = 0.5,
    isActive: Bool = true,
    createdAt: String = "",
    updatedAt: String? = nil,
    applyCount: Int = 0,
    successCount: Int = 0
  ) {
    self.id = id
    self.repoId = repoId
    self.errorSignature = errorSignature
    self.filePattern = filePattern
    self.fixDescription = fixDescription
    self.fixCode = fixCode
    self.source = source
    self.confidence = confidence
    self.isActive = isActive
    self.createdAt = createdAt
    self.updatedAt = updatedAt
    self.applyCount = applyCount
    self.successCount = successCount
  }
}
