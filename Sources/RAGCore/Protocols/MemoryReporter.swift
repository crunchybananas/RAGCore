//
//  MemoryReporter.swift
//  RAGCore
//
//  Optional protocol for memory pressure monitoring during indexing.
//  Decouples RAGCore from MLX or any specific memory management framework.
//

import Foundation

/// Protocol for monitoring and managing memory pressure during indexing.
///
/// RAGCore checks memory pressure periodically during long indexing operations
/// to prevent OOM issues on machines with limited RAM. Implementations can
/// use framework-specific memory APIs (e.g., MLX.Memory, Metal, etc.).
public protocol MemoryPressureMonitor: Sendable {
  /// Returns true if memory pressure is high and indexing should pause/cleanup.
  func isMemoryPressureHigh() -> Bool

  /// Called when memory pressure is detected, to allow cleanup.
  /// Implementations should clear caches, release buffers, etc.
  func clearCaches() async

  /// Returns a snapshot description for logging (e.g., "RSS 2.1 GB, GPU 512 MB").
  func memoryDescription() -> String
}

/// Default no-op implementation when no monitor is provided.
public struct NoOpMemoryMonitor: MemoryPressureMonitor {
  public init() {}

  public func isMemoryPressureHigh() -> Bool { false }
  public func clearCaches() async {}
  public func memoryDescription() -> String { "no monitor" }
}
