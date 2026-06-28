# Design — Neo4j 5 Export / Import

## Goals

Neo4j 5 has no practical offline store-copy/compaction path for this use case, so the database is
rebuilt the supported way: extract everything through the Neo4j Java Driver + Cypher and recreate it
into a clean database.

* Rebuild the database (nodes + relationships, all labels/types/direction/properties).
* Rebuild the indexes and constraints.

Procedure: dump the data and schema from a running source, reset/provision a clean target, restore
the data and schema into the target.

## Implementation Overview

Code lives in `com.brinqa.neo4j.tool` (CLI) and `com.brinqa.neo4j.tool.data` (engine):

* `DumpData` / `LoadData` — picocli commands (`dumpData` / `loadData`), sharing
  `ConnectionOptions`.
* `Exporter` — streams nodes then relationships out of the source.
* `Importer` — recreates nodes then relationships in the target.
* `StagingStore` — disk-backed, partitioned, length-prefixed record files.
* `KryoCodec` — `NodeRecord`/`RelRecord` ⇄ bytes via Kryo.
* `PropertyCodec` — driver value ⇄ Kryo-safe normalized value.
* `Manifest` — `manifest.json` DTO.
* `CypherNames` — safe identifier quoting.

## Schema Discovery

`CALL db.labels()`, `CALL db.relationshipTypes()` and `dbms.components()` feed the manifest. Index
and constraint definitions are dumped to `indexes.jsonl` with the existing `IndexManager`, so the
data and schema dump share one machinery.

## Data Export

Node export and relationship export are independent streamed scans, with driver fetch size bounding
memory. `dumpData` runs them concurrently by default for throughput; `--serial-export` keeps the
older node-then-relationship order for conservative runs against a changing source.

Node scan:

```cypher
MATCH (n) RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props
```

Every node is returned exactly once with all of its labels, so multi-label nodes are not duplicated.
`elementId(n)` is used as the **export-local key** — it is only a cursor/identity *within this single
export* and is never treated as durable identity. Nodes are written to `nodes/<label-set>.kryo`, one
file per label set, so a load batch shares a single label set and its generated `CREATE` Cypher.

> The original sketch iterated label-by-label with key pagination on an indexed `id`. That breaks for
> nodes without `id` and for multi-label nodes (double counting). A single streamed `MATCH (n)` scan
> is simpler, handles every node once, and keeps memory bounded via fetch size.

Relationship scan:

```cypher
MATCH (s)-[r]->(t)
RETURN elementId(r) AS id, elementId(s) AS src, elementId(t) AS dst, type(r) AS type, properties(r) AS props
```

Direction is preserved (`src -> dst`). Records are written to `relationships/<type>.kryo`, one file
per type. Endpoints are referenced by the source element id; there is **no in-memory
elementId→key map**, which is what keeps relationship export memory-bounded on large graphs.

## Import

Order: **nodes, then relationships, then schema.**

Nodes are created in `UNWIND $rows AS row CREATE (...)` batches grouped by label set. Labels and
relationship types are escaped into the Cypher with `CypherNames.quote`; property maps are passed as
parameters (`SET n = row.props`), so property *names* never need escaping.

Relationships need to find their endpoints. Rather than relying on an application `id` (which may be
absent), each node is imported with:

* a temporary `__import_id` property = its export-local key, and
* a temporary `__Imported` label with a uniqueness constraint named `__store_utils_import_id`.

Relationships are then created by matching both endpoints on `__import_id` (constraint-backed).
After all relationships are created the temporary constraint, label and property are removed in
batches (skippable with `--keep-import-keys`). Schema is recreated last via
`IndexManager.loadIndexes` over `indexes.jsonl`.

## Property Serialization

`PropertyCodec` converts driver values into a small canonical form made only of
String/Boolean/Long/Double/byte[]/List/Map so Kryo never has to reflect into the closed `java.base`
module on JDK 17. Temporal, duration and point values become tagged maps
(`{"__t__": "<tag>", ...}`) and are rebuilt into the matching driver type on import, preserving the
Neo4j property type. `KryoCodec` then serializes the records; `FORMAT_VERSION` in the manifest guards
the on-disk format.

## Staging Storage

`StagingStore` is a flat append log: `<dir>/{nodes,relationships}/<partition>.kryo`, each record
length-prefixed. Export→import is a pure sequential write-then-scan, so the random-access /
compaction features of an embedded KV store (RocksDB/speedb, as the original sketch suggested) add
native-dependency risk for no benefit here. Storage is concentrated in this one class, so swapping in
RocksDB later (if random lookups during staging are ever needed) is localized.

## Resumability

The export is a single streamed scan and is not resumable mid-stream. To avoid silent partial
success the manifest is written `IN_PROGRESS` first and flipped to `COMPLETE` only after a clean
finish; `loadData` refuses anything not `COMPLETE`. Keyset pagination on a stable key is the upgrade
path to true resumability.

## Tests

* Unit (no database): `PropertyCodecTest`, `CypherNamesTest`, `ManifestTest`, `StagingStoreTest`.
* Integration (Testcontainers Neo4j 5): `DataRoundTripTest` seeds a graph (multi-label nodes,
  multiple relationship types in both directions, and string/boolean/integer/float/list/temporal/
  point properties plus an index and a constraint), dumps it, wipes the database, loads it back, and
  verifies counts, labels, relationship types, direction, property fidelity, removal of the temporary
  import keys, and schema recreation. It self-skips when Docker is unavailable.
