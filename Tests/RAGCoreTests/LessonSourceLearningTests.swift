@testable import RAGCore
import Foundation
import Testing

/// Referential integrity between a chain learning and the lesson it is mirrored
/// into (cloke/peel#1766).
///
/// The mirror's id is content-addressed, so before `source_learning_id` the only
/// way to delete it alongside its origin was to reconstruct the lesson's
/// `fixDescription` and match on text. That fails quietly in exactly the cases
/// that matter — an edited summary, one summary that is a prefix of another,
/// near-duplicates — which meant a retracted learning kept being served as
/// current. These tests pin the exact-match behaviour that replaces it.
private struct StubEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 3 }
  var modelName: String { "stub" }
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [0, 0, 0] } }
}

@Suite("Lesson source-learning back-reference")
struct LessonSourceLearningTests {

  private func makeStore() async throws -> RAGStore {
    let dbURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-lesson-backref-\(UUID().uuidString).sqlite")
    let store = RAGStore(dbURL: dbURL, embeddingProvider: StubEmbeddingProvider())
    try await store.openIfNeeded()
    try await store.ensureSchema()
    try await store.upsertRepo(id: "r1", name: "alpha", rootPath: "/repo/alpha",
                               lastIndexedAt: nil, repoIdentifier: "github.com/x/alpha")
    return store
  }

  @Test("A bridged lesson is deleted by its origin, not by its text")
  func deletesByOrigin() async throws {
    let store = try await makeStore()
    let learningId = UUID().uuidString
    _ = try await store.addLesson(repoPath: "/repo/alpha",
                                  fixDescription: "Always bound batches by bytes",
                                  sourceLearningId: learningId)

    #expect(try await store.deleteLessons(sourceLearningId: learningId) == 1)
    let remaining = try await store.listLessons(repoPath: "/repo/alpha", includeInactive: true, limit: 50)
    #expect(remaining.isEmpty)
  }

  /// The case content-matching got wrong: one summary being a prefix of another.
  @Test("Deleting one origin leaves a near-identical sibling untouched")
  func siblingWithOverlappingTextSurvives() async throws {
    let store = try await makeStore()
    let keep = UUID().uuidString
    let drop = UUID().uuidString
    _ = try await store.addLesson(repoPath: "/repo/alpha",
                                  fixDescription: "Bound batches by bytes",
                                  sourceLearningId: drop)
    _ = try await store.addLesson(repoPath: "/repo/alpha",
                                  fixDescription: "Bound batches by bytes, not item count",
                                  sourceLearningId: keep)

    #expect(try await store.deleteLessons(sourceLearningId: drop) == 1)
    let remaining = try await store.listLessons(repoPath: "/repo/alpha", includeInactive: true, limit: 50)
    #expect(remaining.count == 1)
    #expect(remaining.first?.fixDescription == "Bound batches by bytes, not item count",
            "a substring match would have taken this one too")
  }

  @Test("All lessons from one origin go together")
  func deletesEveryMirrorOfOneOrigin() async throws {
    let store = try await makeStore()
    let learningId = UUID().uuidString
    for i in 0..<3 {
      _ = try await store.addLesson(repoPath: "/repo/alpha",
                                    fixDescription: "mirror \(i)",
                                    sourceLearningId: learningId)
    }
    #expect(try await store.deleteLessons(sourceLearningId: learningId) == 3)
    #expect(try await store.listLessons(repoPath: "/repo/alpha", includeInactive: true, limit: 50).isEmpty)
  }

  @Test("Lessons authored directly are never collateral")
  func unbridgedLessonsAreUntouched() async throws {
    let store = try await makeStore()
    let learningId = UUID().uuidString
    _ = try await store.addLesson(repoPath: "/repo/alpha", fixDescription: "bridged",
                                  sourceLearningId: learningId)
    _ = try await store.addLesson(repoPath: "/repo/alpha", fixDescription: "hand-written")

    #expect(try await store.deleteLessons(sourceLearningId: learningId) == 1)
    let remaining = try await store.listLessons(repoPath: "/repo/alpha", includeInactive: true, limit: 50)
    #expect(remaining.map(\.fixDescription) == ["hand-written"])
  }

  /// 0 is a legitimate answer — the learning may never have been bridged
  /// (only repository scope is), or predate the back-reference. The caller
  /// uses that to decide whether to fall back to content matching.
  @Test("An unknown origin deletes nothing and reports zero")
  func unknownOriginIsNotAnError() async throws {
    let store = try await makeStore()
    _ = try await store.addLesson(repoPath: "/repo/alpha", fixDescription: "unrelated")
    #expect(try await store.deleteLessons(sourceLearningId: UUID().uuidString) == 0)
    #expect(try await store.listLessons(repoPath: "/repo/alpha", includeInactive: true, limit: 50).count == 1)
  }

  @Test("Legacy rows written before the back-reference keep working")
  func legacyRowsHaveNoOrigin() async throws {
    let store = try await makeStore()
    // No sourceLearningId — models a row written pre-migration.
    _ = try await store.addLesson(repoPath: "/repo/alpha", fixDescription: "legacy row")
    // Must not be swept by an empty/nil-ish origin lookup.
    #expect(try await store.deleteLessons(sourceLearningId: "") == 0)
    #expect(try await store.listLessons(repoPath: "/repo/alpha", includeInactive: true, limit: 50).count == 1)
  }
}
