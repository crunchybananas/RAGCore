//
//  VectorMath.swift
//  RAGCore
//
//  Vector encoding/decoding and similarity computation utilities.
//

import CryptoKit
import Foundation

/// Utilities for vector operations used by the RAG storage layer.
public enum VectorMath {

  /// Encode a Float vector to a raw byte blob for SQLite storage.
  public static func encodeVector(_ vector: [Float]) -> Data {
    var copy = vector
    return Data(bytes: &copy, count: MemoryLayout<Float>.stride * copy.count)
  }

  /// Decode a raw byte blob back to a Float vector.
  public static func decodeVector(_ data: Data) -> [Float]? {
    let stride = MemoryLayout<Float>.stride
    guard data.count % stride == 0 else { return nil }
    let count = data.count / stride
    return data.withUnsafeBytes { buffer in
      let pointer = buffer.bindMemory(to: Float.self)
      guard pointer.count >= count else { return nil }
      return Array(pointer.prefix(count))
    }
  }

  /// Compute cosine similarity between two vectors.
  public static func cosineSimilarity(_ lhs: [Float], _ rhs: [Float]) -> Float {
    let count = min(lhs.count, rhs.count)
    guard count > 0 else { return 0 }
    var dot: Float = 0
    var lhsSum: Float = 0
    var rhsSum: Float = 0
    for i in 0..<count {
      dot += lhs[i] * rhs[i]
      lhsSum += lhs[i] * lhs[i]
      rhsSum += rhs[i] * rhs[i]
    }
    let denom = sqrt(max(lhsSum * rhsSum, 0.000001))
    return dot / denom
  }

  /// Compute a stable SHA-256-based ID for a string value.
  public static func stableId(for value: String) -> String {
    let digest = SHA256.hash(data: Data(value.utf8))
    return digest.map { String(format: "%02x", $0) }.joined()
  }
}
