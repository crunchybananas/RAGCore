# RAGCore 3.0 storage ownership

RAGCore now owns and exports `CSQLite`. Its `SQLiteVec` and `RAGCore` targets use
that local target, and the package no longer depends on MCPCore. The existing
SQLite source, headers, compiler options, and linker settings were transferred
unchanged from MCPCore 1.6.0. No database schema or stored-data migration is needed
for this ownership change.

## Consumers

Use RAGCore `from: "3.0.0"` and, if MCP protocol
types are needed, MCPCore `from: "2.0.0"`.

```swift
.target(name: "YourApp", dependencies: [
  .product(name: "RAGCore", package: "RAGCore"),
  .product(name: "CSQLite", package: "RAGCore"), // only for direct SQLite access
  .product(name: "MCPCore", package: "MCPCore"), // only for MCP protocol types
])
```

Existing `import RAGCore` and `import CSQLite` statements can remain. Code that
previously received SQLite symbols implicitly through `import MCPCore` must add
`import CSQLite` and a direct CSQLite product dependency. MCPCore 2.0 does not
re-export storage.

Do not combine RAGCore 3.x with MCPCore 1.x: both would provide a `CSQLite` module.
The major version keeps existing RAGCore 2.x/MCPCore 1.x consumers on their original
graph until they deliberately upgrade. Use the paired release tags; local paths or exact reviewed commits are also suitable for development validation.

## Peel upgrade checklist

Peel's project and its local Git package currently request MCPCore 1.x. Update
both to 2.x and RAGCore to 3.x together. Add RAGCore's CSQLite product explicitly
to each application/test target importing it. Resolve the dependency graph once,
verify that only RAGCore owns CSQLite, then run Peel's coordinated app/package
builds and its database-facing tests. Do not update only one MCPCore requirement
or claim a Peel build based on package tests alone.

The existing Peel source, lockfiles, running app, and databases are unchanged by
this package migration. The storage split is tagged as RAGCore 3.0.0 and MCPCore 2.0.0.
