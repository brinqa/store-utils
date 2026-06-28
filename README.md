# Store-Utils: Neo4j 5 Export / Import & Index Maintenance

This project was inspired by Michael Hunger's *store-utils* and is a continuation of the 4.x
community effort. The 4.x line copied the database at the storage-file level. Neo4j 5 changed those
internals and no longer offers a practical offline compaction path for this use case, so the 5.x
line rebuilds a database the supported way: it **exports the graph through the Neo4j Java Driver and
Cypher, then recreates it into a clean database.**

## Use Cases

Compaction/optimization of a long-running database, or recovery after a large deletion. Neo4j will
try to reclaim space on its own, but rebuilding into a fresh database reliably defragments the store
and restores performance. Rebuilding also drops orphaned/corrupt artifacts that a live database
keeps dragging along.

Examples:
* A burst of data was deleted and the store is now abnormally large.
* A large clean-up of no-longer-used data is required.

## How It Works

The rebuild is four steps, exposed as CLI commands:

1. `dumpData` – stream all nodes, relationships and the schema (indexes/constraints) out of the
   source database into a local **staging directory**.
2. Reset / provision a clean, empty target database.
3. `loadData` – recreate the nodes and relationships in the target, then recreate the indexes and
   constraints.
4. (Indexes only, if you prefer to manage them separately: `dump` / `load`.)

### Staging Layout

`dumpData` writes a self-describing directory:

```text
<dump-dir>/
  manifest.json          # tool/server version, timestamps, counts, batch size, format version
  indexes.jsonl          # index & constraint definitions (same format as `dump`)
  nodes/<label-set>.kryo  # length-prefixed, Kryo-serialized node records, one file per label set
  relationships/<type>.kryo  # length-prefixed, Kryo-serialized relationship records, one per type
```

Records are streamed to disk (one file per label-set / relationship-type) and read back one at a
time, so memory stays bounded regardless of graph size. Property values are normalized to plain
JDK/driver types before serialization (temporal, duration and point values are tagged so they keep
their Neo4j type on import).

### Identity

Application-level identity is preserved by re-creating every node with all of its original labels
and properties. To wire relationships back to the correct endpoints, each node is imported with a
**temporary** `__import_id` property (carrying the source element id, which is only meaningful within
a single export) and a temporary `__Imported` label backed by a temporary uniqueness constraint. After
relationships are created these temporary keys are removed (use `--keep-import-keys` to retain them
for debugging or resume). The manifest records whether an indexed application `id` property was
found; the import key approach is used uniformly so nodes without an `id` are handled too.

## Prerequisites

* Java 17.
* A running source Neo4j 5 database and a **separate, empty** target Neo4j 5 database (Community
  edition only exposes the `neo4j` and `system` databases, so the target is usually a second
  instance, or the same instance after it has been wiped).
* Enough local disk for the staging directory (roughly proportional to the data size).
* Credentials for both databases.

## Commands

All commands authenticate with these environment variables (or the matching flags):

```text
NEO4J_URL        # default bolt://localhost:7687
NEO4J_USERNAME
NEO4J_PASSWORD
```

Flags: `-a/--url`, `-u/--username`, `-p/--password`, `-n/--no_auth`. Every command supports
`-h/--help`.

### 1. Export the source database

```bash
$ ./bin/dumpData --output /data/dump
```

Writes `manifest.json`, `indexes.jsonl`, `nodes/` and `relationships/` into `/data/dump`. By default
the node scan and relationship scan run as two concurrent read streams; use `--serial-export` to run
them one after the other. Tune the streaming/fetch batch with `-b/--batch` (default 10000). A crashed
dump leaves the manifest marked `IN_PROGRESS`; `loadData` refuses to load anything that is not
`COMPLETE`, so partial dumps can never be mistaken for good ones.

### 2. Reset the target database

The target must be **empty**. For Community, stop the instance, remove/replace the database, and
start a fresh one (or `MATCH (n) DETACH DELETE n` plus dropping constraints/indexes on a scratch
instance). Importing into a non-empty database will create duplicates.

### 3. Import into the target

Point the connection at the **target** and run:

```bash
$ NEO4J_URL=bolt://target:7687 ./bin/loadData --input /data/dump
```

This imports nodes, then relationships, removes the temporary import keys, and finally recreates the
indexes/constraints from `indexes.jsonl`. Options:

* `-b/--batch` – `UNWIND CREATE` batch size (default 10000).
* `-p/--parallelism` – number of staged node/relationship partitions to import concurrently.
* `--skip-indexes` – import data only; recreate indexes later with `load`.
* `--keep-import-keys` – leave the `__import_id`/`__Imported` scaffolding in place.

## Index / Constraint Only Workflow

If you only need to dump and restore the schema (for example, after recovering a store some other
way), use the original index commands. The file is newline-delimited JSON; you can delete a line to
skip that index/constraint.

Dump the definitions (Neo4j must be running):

```bash
$ ./bin/dump            # writes dump.jsonl
```

Restore them in a controlled manner (each index is created and brought online before the next, to
avoid overwhelming Neo4j):

```bash
$ ./bin/load -f dump.jsonl
```

`load` skips indexes/constraints that already exist by name. Use `-r/--refresh` to drop and recreate
(see Known Issues — use sparingly). `-d/--dryrun` prints the generated Cypher without executing it.

## Failure Recovery

* **Dump interrupted** – rerun `dumpData` (it overwrites the staging directory). The manifest stays
  `IN_PROGRESS` until the dump finishes successfully.
* **Load interrupted before relationships finish** – the target is partial. Wipe it and rerun
  `loadData`. The staging directory is unchanged and reusable.
* **Index creation flaky** – rerun with `load` (it skips already-online indexes), or create the
  problematic index manually and remove its line from `indexes.jsonl`.

## Limitations

* Map-valued properties and `byte[]` are not native Neo4j node property types; only Neo4j-supported
  property values (strings, booleans, integers, floats, their arrays, temporal/duration/point) are
  exercised end-to-end.
* The export is a single streamed scan per entity and is **not** resumable mid-stream; on failure
  rerun the dump. Keyset pagination would make it resumable and is the natural upgrade path.
* The convenience `copyData` (run export+import in one shot against two connections) is intentionally
  not implemented — run `dumpData` then `loadData`. It is a small addition if dual-connection
  ergonomics are wanted later.
* Relationship-scoped indexes/constraints and multi-label full-text indexes are not generated by the
  index machinery (node indexes, single-label full-text, and uniqueness constraints are).

## Known Issues

* `load -r/--refresh` (drop + recreate) can fail intermittently — Neo4j may randomly fail a
  drop/create of an index/constraint. Prefer the default skip-if-exists behavior and recreate
  problem indexes manually.
* Neo4j can occasionally show a unique index that does not appear under `SHOW CONSTRAINTS`. Drop the
  index and recreate the constraint manually with the same name so `load` can skip it.
