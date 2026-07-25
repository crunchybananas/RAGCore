//
//  SubrepoExclusionTests.swift
//  RAGCoreTests
//
//  Pins which sub-repo roots a scan skips (cloke/peel double-index).
//
//  A superproject with 13 submodules had 5,479 of its 7,732 files
//  byte-identical to a file in a child repo, because a flat workspace index
//  walked straight into checkouts that already had their own rows. Duplicated
//  embeddings, duplicated analysis spend, and two search hits for every match
//  inside a submodule.
//

@testable import RAGCore
import Testing

@Suite("Sub-repo exclusion")
struct SubrepoExclusionTests {
  private let subrepos = ["/ws/tio-api", "/ws/tio-front-end", "/ws/untracked-thing"]

  private func isTracked(_ path: String) -> Bool {
    path == "/ws/tio-api" || path == "/ws/tio-front-end"
  }

  @Test("explicit exclude skips every sub-repo, tracked or not")
  func explicitExcludeSkipsAll() {
    let excluded = RAGStore.excludedSubrepoRoots(
      workspaceRepos: subrepos,
      allowWorkspace: true,
      excludeSubrepos: true,
      isTracked: isTracked
    )
    #expect(excluded == subrepos)
  }

  @Test("flat workspace still skips sub-repos that already have their own row")
  func flatWorkspaceSkipsTracked() {
    // The regression. `excludeSubrepos: false` used to mean "walk everything",
    // which duplicated every tracked child into the parent.
    let excluded = RAGStore.excludedSubrepoRoots(
      workspaceRepos: subrepos,
      allowWorkspace: true,
      excludeSubrepos: false,
      isTracked: isTracked
    )
    #expect(excluded.sorted() == ["/ws/tio-api", "/ws/tio-front-end"])
  }

  @Test("flat workspace still indexes untracked sub-repos")
  func flatWorkspaceKeepsUntracked() {
    // The flag keeps its purpose: a vendored checkout nobody indexes separately
    // belongs in the parent's index.
    let excluded = RAGStore.excludedSubrepoRoots(
      workspaceRepos: subrepos,
      allowWorkspace: true,
      excludeSubrepos: false,
      isTracked: isTracked
    )
    #expect(!excluded.contains("/ws/untracked-thing"))
  }

  @Test("a lone tracked child is excluded even without allowWorkspace")
  func singleTrackedChildIsExcluded() {
    // The workspace split only fires at two or more sub-repos, so a parent with
    // exactly one tracked child fell through to a normal scan and absorbed it
    // with no flag involved.
    let excluded = RAGStore.excludedSubrepoRoots(
      workspaceRepos: ["/ws/tio-api"],
      allowWorkspace: false,
      excludeSubrepos: true,
      isTracked: isTracked
    )
    #expect(excluded == ["/ws/tio-api"])
  }

  @Test("an untracked lone child is still indexed into its parent")
  func singleUntrackedChildIsIndexed() {
    let excluded = RAGStore.excludedSubrepoRoots(
      workspaceRepos: ["/ws/untracked-thing"],
      allowWorkspace: false,
      excludeSubrepos: true,
      isTracked: isTracked
    )
    #expect(excluded.isEmpty)
  }

  @Test("no sub-repos means nothing to exclude")
  func noSubrepos() {
    for allowWorkspace in [true, false] {
      for excludeSubrepos in [true, false] {
        let excluded = RAGStore.excludedSubrepoRoots(
          workspaceRepos: [],
          allowWorkspace: allowWorkspace,
          excludeSubrepos: excludeSubrepos,
          isTracked: isTracked
        )
        #expect(excluded.isEmpty)
      }
    }
  }

  @Test("exclusion never invents a root the caller did not report")
  func excludedIsAlwaysASubset() {
    for allowWorkspace in [true, false] {
      for excludeSubrepos in [true, false] {
        let excluded = RAGStore.excludedSubrepoRoots(
          workspaceRepos: subrepos,
          allowWorkspace: allowWorkspace,
          excludeSubrepos: excludeSubrepos,
          isTracked: isTracked
        )
        #expect(Set(excluded).isSubset(of: Set(subrepos)))
      }
    }
  }
}
