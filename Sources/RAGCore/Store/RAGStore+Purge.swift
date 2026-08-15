//
//  RAGStore+Purge.swift
//  RAGCore
//
//  The retro-active half of the credential boundary (#11): the scan-time
//  policy stops NEW credential reads, but a corpus indexed before the policy
//  existed already holds the rows. Audit names them; purge removes them.
//

import Foundation
import SQLite3

/// What a policy purge found and removed. Paths and counts only — the purge
/// exists because contents should never have been stored, so it certainly
/// never echoes them.
public struct RAGPolicyPurgeReport: Sendable {
  /// Root-relative (as-stored) paths whose rows were removed.
  public let purgedPaths: [String]
  /// Chunk rows removed across those files.
  public let chunksRemoved: Int
}

extension RAGStore {

  /// Paths in this repo's index that the credential policy would refuse
  /// today — the audit half of #11. Read-only; never touches file contents
  /// (the `files` table's own metadata answers everything).
  public func auditPolicyExclusions(
    repoId: String,
    policy: CredentialExclusionPolicy = .standard
  ) throws -> [String] {
    try openIfNeeded()
    try ensureSchema()
    return try indexedFiles(repoId: repoId)
      .filter { policy.excludes(relativePath: $0.path, fileName: ($0.path as NSString).lastPathComponent) }
      .map(\.path)
  }

  /// Remove every indexed row for files the credential policy refuses —
  /// file row, chunks, embeddings, vector rows, chunk analysis, symbols,
  /// symbol references, and dependencies. Run after a policy update so the
  /// index converges on what the policy would ingest today (#11).
  public func purgePolicyExclusions(
    repoId: String,
    policy: CredentialExclusionPolicy = .standard
  ) throws -> RAGPolicyPurgeReport {
    try openIfNeeded()
    try ensureSchema()

    let doomed = try indexedFiles(repoId: repoId).filter {
      policy.excludes(relativePath: $0.path, fileName: ($0.path as NSString).lastPathComponent)
    }
    guard !doomed.isEmpty else {
      return RAGPolicyPurgeReport(purgedPaths: [], chunksRemoved: 0)
    }

    var chunksRemoved = 0
    for file in doomed {
      chunksRemoved += try chunkCount(fileId: file.id)
      // One function owns "remove a file's chunks and everything keyed on
      // them" (see deleteChunks) — duplicating its child-table list is how
      // the #19 embedding leak happened.
      try deleteChunks(for: file.id)
      try deleteDependencies(for: file.id)
      try deleteSymbolRefs(for: file.id)
      try deleteSymbols(for: file.id)
      try execute(sql: "DELETE FROM files WHERE id = ?") { stmt in
        bindText(stmt, 1, file.id)
      }
    }
    return RAGPolicyPurgeReport(
      purgedPaths: doomed.map(\.path),
      chunksRemoved: chunksRemoved
    )
  }

  // MARK: - Private

  private func indexedFiles(repoId: String) throws -> [(id: String, path: String)] {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = "SELECT id, path FROM files WHERE repo_id = ?"
    var statement: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }
    bindText(statement, 1, repoId)
    var rows: [(id: String, path: String)] = []
    while sqlite3_step(statement) == SQLITE_ROW {
      guard let idPtr = sqlite3_column_text(statement, 0),
            let pathPtr = sqlite3_column_text(statement, 1) else { continue }
      rows.append((id: String(cString: idPtr), path: String(cString: pathPtr)))
    }
    return rows
  }

  private func chunkCount(fileId: String) throws -> Int {
    guard let db else { throw RAGError.sqlite("Database not initialized") }
    let sql = "SELECT COUNT(*) FROM chunks WHERE file_id = ?"
    var statement: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
      throw RAGError.sqlite(String(cString: sqlite3_errmsg(db)))
    }
    defer { sqlite3_finalize(statement) }
    bindText(statement, 1, fileId)
    guard sqlite3_step(statement) == SQLITE_ROW else { return 0 }
    return Int(sqlite3_column_int64(statement, 0))
  }
}
