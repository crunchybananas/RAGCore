//
//  RAGStore+Integrity.swift
//  RAGCore
//
//  Referential integrity for a store that does not enforce it.
//
//  The schema declares plain `FOREIGN KEY (...) REFERENCES ...` with no
//  `ON DELETE CASCADE`, and SQLite's `foreign_keys` pragma defaults to OFF and
//  is never enabled anywhere in RAGCore. Every delete path therefore has to
//  remove child rows by hand, and any path that forgets leaves rows behind
//  with nothing pointing at them.
//
//  One such path (`pruneDeletedFiles`) ran on every index refresh and left
//  11,218 dangling chunks carrying 10,906 embeddings on a single machine
//  before it was found by hand (cloke/peel#1881). This makes that condition
//  reportable instead of discoverable only by writing SQL against a live
//  database.
//

import CSQLite
import Foundation

/// Rows that survive with nothing referencing them.
///
/// Counts are whole-store, not per-repo: an orphan has no file row, so it has
/// no repo either, which is exactly why per-repo counts cannot see it.
public struct RAGStoreIntegrityReport: Sendable, Equatable {
  /// Chunks whose `file_id` matches no row in `files`.
  public let danglingChunks: Int
  /// Embeddings whose `chunk_id` matches no row in `chunks`, INCLUDING those
  /// reachable only through a dangling chunk.
  public let danglingEmbeddings: Int

  public init(danglingChunks: Int, danglingEmbeddings: Int) {
    self.danglingChunks = danglingChunks
    self.danglingEmbeddings = danglingEmbeddings
  }

  public var isClean: Bool { danglingChunks == 0 && danglingEmbeddings == 0 }

  public var summary: String {
    isClean
      ? "No dangling rows"
      : "\(danglingChunks) dangling chunk(s), \(danglingEmbeddings) dangling embedding(s)"
  }
}

extension RAGStore {

  /// Count rows that nothing references. Read-only.
  public func integrityReport() throws -> RAGStoreIntegrityReport {
    try openIfNeeded()
    guard let db else { throw RAGError.sqlite("Database not initialized") }

    let chunks = try Self.scalar(
      db,
      "SELECT COUNT(*) FROM chunks WHERE file_id NOT IN (SELECT id FROM files)",
      label: "danglingChunks"
    )
    // Deliberately not "embeddings whose chunk is dangling": an embedding whose
    // chunk row is gone entirely is just as orphaned, and counting only the
    // first kind is how a partial sweep reports itself as clean.
    let embeddings = try Self.scalar(
      db,
      """
      SELECT COUNT(*) FROM embeddings
      WHERE chunk_id NOT IN (
        SELECT id FROM chunks WHERE file_id IN (SELECT id FROM files)
      )
      """,
      label: "danglingEmbeddings"
    )
    return RAGStoreIntegrityReport(danglingChunks: chunks, danglingEmbeddings: embeddings)
  }

  /// Delete every unreferenced row and return what was removed.
  ///
  /// Safe by construction: nothing can legitimately reference a chunk whose
  /// file is gone, or an embedding whose chunk is gone. Embeddings go first so
  /// that deleting the chunks cannot strand them a second time.
  ///
  /// Returns the counts REMOVED, not the counts remaining. A caller that logs
  /// this is reporting work done; to assert the store is clean afterwards,
  /// call `integrityReport()` again.
  @discardableResult
  public func deleteDanglingRows() throws -> RAGStoreIntegrityReport {
    let before = try integrityReport()
    guard !before.isClean else { return before }

    if extensionLoaded {
      try execute(sql: """
        DELETE FROM vec_chunks WHERE chunk_id NOT IN (
          SELECT id FROM chunks WHERE file_id IN (SELECT id FROM files)
        )
        """) { _ in }
    }
    try execute(sql: """
      DELETE FROM embeddings WHERE chunk_id NOT IN (
        SELECT id FROM chunks WHERE file_id IN (SELECT id FROM files)
      )
      """) { _ in }
    try execute(sql: "DELETE FROM chunks WHERE file_id NOT IN (SELECT id FROM files)") { _ in }

    return before
  }

  private static func scalar(_ db: OpaquePointer, _ sql: String, label: String) throws -> Int {
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
      throw RAGError.sqlite("\(label) prepare failed: \(String(cString: sqlite3_errmsg(db)))")
    }
    defer { sqlite3_finalize(stmt) }
    return sqlite3_step(stmt) == SQLITE_ROW ? Int(sqlite3_column_int(stmt, 0)) : 0
  }
}
