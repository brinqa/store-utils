/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.brinqa.neo4j.tool.data;

import static com.brinqa.neo4j.tool.util.Print.println;

import com.brinqa.neo4j.tool.util.CypherNames;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

/**
 * Recreates a graph in a clean Neo4j 5 database from a {@link StagingStore}.
 *
 * <p>Nodes are imported first, each tagged with a temporary {@link Manifest#IMPORT_LABEL} label and
 * a {@link Manifest#IMPORT_ID} property carrying its export-local key. A temporary uniqueness
 * constraint on that property lets relationship import look endpoints up cheaply. After
 * relationships are created the temporary label/property/constraint are removed (unless {@code
 * keepImportKeys} is set, e.g. to resume).
 *
 * <p>Labels and relationship types are escaped into the generated Cypher; all property values are
 * passed as parameters so property names never need escaping.
 */
public class Importer {

  private static final String IMPORT_CONSTRAINT = "__store_utils_import_id";

  private final Driver driver;
  private final StagingStore store;
  private final int batchSize;
  private final boolean keepImportKeys;
  private final int parallelism;

  public Importer(Driver driver, StagingStore store, int batchSize, boolean keepImportKeys) {
    this(driver, store, batchSize, keepImportKeys, 1);
  }

  public Importer(
      Driver driver, StagingStore store, int batchSize, boolean keepImportKeys, int parallelism) {
    this.driver = driver;
    this.store = store;
    this.batchSize = batchSize;
    this.keepImportKeys = keepImportKeys;
    this.parallelism = Math.max(1, parallelism);
  }

  public long importNodes() {
    if (parallelism > 1) {
      final var partitions = store.nodePartitions();
      println("Importing %d node partitions with %d workers", partitions.size(), parallelism);
      return runParallel(partitions, this::importNodePartition);
    }
    return importNodesSerial();
  }

  public long importRelationships() {
    awaitImportConstraint();
    final long total;
    if (parallelism > 1) {
      final var partitions = store.relPartitions();
      println(
          "Importing %d relationship partitions with %d workers", partitions.size(), parallelism);
      total = runParallel(partitions, this::importRelPartition);
    } else {
      total = importRelationshipsSerial();
    }
    if (!keepImportKeys) {
      cleanup();
    }
    return total;
  }

  private long importNodesSerial() {
    final var batch = new Batch();
    final var currentLabels = new AtomicReference<List<String>>();
    store.forEachNode(
        node -> {
          if (currentLabels.get() != null && !currentLabels.get().equals(node.getLabels())) {
            flushNodes(currentLabels.get(), batch);
          }
          currentLabels.set(node.getLabels());
          final var row = new LinkedHashMap<String, Object>();
          row.put("key", node.getKey());
          row.put("props", denormalize(node.getProps()));
          batch.add(row);
          if (batch.size() >= batchSize) {
            flushNodes(currentLabels.get(), batch);
          }
        });
    if (!batch.isEmpty()) {
      flushNodes(currentLabels.get(), batch);
    }
    return batch.total();
  }

  private long importRelationshipsSerial() {
    final var batch = new Batch();
    final var currentType = new AtomicReference<String>();
    store.forEachRel(
        rel -> {
          if (currentType.get() != null && !currentType.get().equals(rel.getType())) {
            flushRels(currentType.get(), batch);
          }
          currentType.set(rel.getType());
          final var row = new LinkedHashMap<String, Object>();
          row.put("src", rel.getSrc());
          row.put("dst", rel.getDst());
          row.put("props", denormalize(rel.getProps()));
          batch.add(row);
          if (batch.size() >= batchSize) {
            flushRels(currentType.get(), batch);
          }
        });
    if (!batch.isEmpty()) {
      flushRels(currentType.get(), batch);
    }
    return batch.total();
  }

  private long importNodePartition(File partition) {
    final var batch = new Batch();
    final var currentLabels = new AtomicReference<List<String>>();
    store.forEachNode(
        partition,
        node -> {
          if (currentLabels.get() != null && !currentLabels.get().equals(node.getLabels())) {
            flushNodes(currentLabels.get(), batch);
          }
          currentLabels.set(node.getLabels());
          final var row = new LinkedHashMap<String, Object>();
          row.put("key", node.getKey());
          row.put("props", denormalize(node.getProps()));
          batch.add(row);
          if (batch.size() >= batchSize) {
            flushNodes(currentLabels.get(), batch);
          }
        });
    if (!batch.isEmpty()) {
      flushNodes(currentLabels.get(), batch);
    }
    return batch.total();
  }

  private long importRelPartition(File partition) {
    final var batch = new Batch();
    final var currentType = new AtomicReference<String>();
    store.forEachRel(
        partition,
        rel -> {
          if (currentType.get() != null && !currentType.get().equals(rel.getType())) {
            flushRels(currentType.get(), batch);
          }
          currentType.set(rel.getType());
          final var row = new LinkedHashMap<String, Object>();
          row.put("src", rel.getSrc());
          row.put("dst", rel.getDst());
          row.put("props", denormalize(rel.getProps()));
          batch.add(row);
          if (batch.size() >= batchSize) {
            flushRels(currentType.get(), batch);
          }
        });
    if (!batch.isEmpty()) {
      flushRels(currentType.get(), batch);
    }
    return batch.total();
  }

  private long runParallel(List<File> partitions, ToLongFunction<File> worker) {
    if (partitions.isEmpty()) {
      return 0;
    }
    final var tasks =
        partitions.stream()
            .<Callable<Long>>map(partition -> () -> worker.applyAsLong(partition))
            .toList();
    final var executor = Executors.newFixedThreadPool(Math.min(parallelism, tasks.size()));
    try {
      long total = 0;
      for (final var future : executor.invokeAll(tasks)) {
        total += future.get();
      }
      return total;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
      throw new IllegalStateException("Parallel import interrupted", e);
    } catch (ExecutionException e) {
      executor.shutdownNow();
      throw new IllegalStateException("Parallel import failed", e.getCause());
    } finally {
      executor.shutdown();
    }
  }

  private void flushNodes(List<String> labels, Batch batch) {
    if (batch.isEmpty()) {
      return;
    }
    final var labelChain =
        CypherNames.labels(labels) + ":" + CypherNames.quote(Manifest.IMPORT_LABEL);
    final var query =
        "UNWIND $rows AS row CREATE (n"
            + labelChain
            + ") SET n = row.props SET n."
            + CypherNames.quote(Manifest.IMPORT_ID)
            + " = row.key";
    write(query, batch.drain());
  }

  private void flushRels(String type, Batch batch) {
    if (batch.isEmpty()) {
      return;
    }
    final var importLabel = CypherNames.quote(Manifest.IMPORT_LABEL);
    final var importId = CypherNames.quote(Manifest.IMPORT_ID);
    final var query =
        "UNWIND $rows AS row"
            + " MATCH (s:"
            + importLabel
            + " {"
            + importId
            + ": row.src})"
            + " MATCH (t:"
            + importLabel
            + " {"
            + importId
            + ": row.dst})"
            + " CREATE (s)-[r:"
            + CypherNames.quote(type)
            + "]->(t) SET r = row.props";
    write(query, batch.drain());
  }

  private void awaitImportConstraint() {
    final var query =
        "CREATE CONSTRAINT "
            + CypherNames.quote(IMPORT_CONSTRAINT)
            + " IF NOT EXISTS FOR (n:"
            + CypherNames.quote(Manifest.IMPORT_LABEL)
            + ") REQUIRE n."
            + CypherNames.quote(Manifest.IMPORT_ID)
            + " IS UNIQUE";
    runWrite(query, Map.of());
    runWrite("CALL db.awaitIndexes(600)", Map.of());
  }

  private void cleanup() {
    println("Removing temporary import keys");
    runWrite("DROP CONSTRAINT " + CypherNames.quote(IMPORT_CONSTRAINT) + " IF EXISTS", Map.of());
    final var query =
        "MATCH (n:"
            + CypherNames.quote(Manifest.IMPORT_LABEL)
            + ") WITH n LIMIT $limit REMOVE n:"
            + CypherNames.quote(Manifest.IMPORT_LABEL)
            + ", n."
            + CypherNames.quote(Manifest.IMPORT_ID)
            + " RETURN count(*) AS c";
    long removed;
    do {
      removed = runWriteCount(query, Map.of("limit", batchSize));
    } while (removed > 0);
  }

  private void write(String query, List<Map<String, Object>> rows) {
    runWrite(query, Map.of("rows", rows));
  }

  private void runWrite(String query, Map<String, Object> params) {
    try (Session session = driver.session()) {
      session.executeWrite(tx -> tx.run(query, params).consume());
    }
  }

  private long runWriteCount(String query, Map<String, Object> params) {
    try (Session session = driver.session()) {
      return session.executeWrite(
          tx -> {
            final var r = tx.run(query, params);
            return r.hasNext() ? r.next().get("c").asLong(0) : 0L;
          });
    }
  }

  private static Map<String, Object> denormalize(Map<String, Object> props) {
    final var out = new LinkedHashMap<String, Object>(props.size());
    props.forEach((k, v) -> out.put(k, PropertyCodec.denormalize(v)));
    return out;
  }

  /** Accumulates rows for one UNWIND batch and tracks a running total across flushes. */
  private static final class Batch {
    private List<Map<String, Object>> rows = new ArrayList<>();
    private long total;

    void add(Map<String, Object> row) {
      rows.add(row);
    }

    int size() {
      return rows.size();
    }

    boolean isEmpty() {
      return rows.isEmpty();
    }

    long total() {
      return total;
    }

    List<Map<String, Object>> drain() {
      final var drained = rows;
      total += drained.size();
      rows = new ArrayList<>();
      return drained;
    }
  }
}
