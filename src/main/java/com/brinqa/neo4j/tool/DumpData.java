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
package com.brinqa.neo4j.tool;

import static com.brinqa.neo4j.tool.util.Print.println;

import com.brinqa.neo4j.tool.data.Exporter;
import com.brinqa.neo4j.tool.data.Manifest;
import com.brinqa.neo4j.tool.data.StagingStore;
import com.brinqa.neo4j.tool.index.IndexManager;
import com.brinqa.neo4j.tool.util.Neo4jHelper;
import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.neo4j.driver.Driver;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Exports a running Neo4j 5 database (schema, nodes, relationships) into a local staging directory
 * that {@link LoadData} can replay into a clean database.
 */
@Command(
    name = "dumpData",
    version = "dumpData 1.0",
    description = "Export nodes, relationships, indexes and a manifest into a staging directory.")
public class DumpData implements Runnable {

  @Mixin ConnectionOptions conn;

  @Option(
      names = {"-o", "--output"},
      description = "Staging directory to write the dump into.",
      defaultValue = "dump")
  File output;

  @Option(
      names = {"-b", "--batch"},
      description = "Batch / fetch size used while streaming.",
      defaultValue = "10000")
  int batchSize;

  @Option(
      names = {"--serial-export"},
      description =
          "Export nodes then relationships sequentially instead of using two read streams.")
  boolean serialExport;

  public static void main(String... args) {
    System.exit(new CommandLine(new DumpData()).execute(args));
  }

  @Override
  public void run() {
    if (!output.isDirectory() && !output.mkdirs()) {
      throw new IllegalStateException("Unable to create output directory: " + output);
    }
    try (final var driver = conn.buildDriver()) {
      final var indexManager = new IndexManager(driver);
      final var store = new StagingStore(output);
      store.init();

      final var labels = new ArrayList<>(Neo4jHelper.readAllLabels(driver));
      final var relTypes = new ArrayList<>(Neo4jHelper.readAllRelationshipTypes(driver));
      final var version = Neo4jHelper.serverVersion(driver);
      final var indexes = indexManager.readIndexes();
      indexManager.writeIndexes(new File(output, "indexes.jsonl"), indexes);

      final var started = Instant.now().toString();
      // Write an in-progress manifest first so a crashed dump is never mistaken for complete.
      manifest(version, started, started, "IN_PROGRESS", labels, relTypes, indexes, 0, 0)
          .write(output);

      final var counts = serialExport ? exportSerial(driver, store) : exportParallel(driver, store);

      final var finished = Instant.now().toString();
      manifest(
              version,
              started,
              finished,
              "COMPLETE",
              labels,
              relTypes,
              indexes,
              counts.nodeCount(),
              counts.relCount())
          .write(output);
      println("Dump complete: %s", output.getAbsolutePath());
    }
  }

  private ExportCounts exportSerial(Driver driver, StagingStore store) {
    println("Exporting nodes from %s", conn.uri);
    final long nodeCount = new Exporter(driver, store, batchSize).exportNodes();
    println("Exported %d nodes", nodeCount);

    println("Exporting relationships");
    final long relCount = new Exporter(driver, store, batchSize).exportRelationships();
    println("Exported %d relationships", relCount);
    return new ExportCounts(nodeCount, relCount);
  }

  private ExportCounts exportParallel(Driver driver, StagingStore store) {
    println("Exporting nodes and relationships from %s using 2 read streams", conn.uri);
    final var executor = Executors.newFixedThreadPool(2);
    try {
      final Future<Long> nodes =
          executor.submit(
              () -> {
                final long count = new Exporter(driver, store, batchSize).exportNodes();
                println("Exported %d nodes", count);
                return count;
              });
      final Future<Long> rels =
          executor.submit(
              () -> {
                final long count = new Exporter(driver, store, batchSize).exportRelationships();
                println("Exported %d relationships", count);
                return count;
              });
      return new ExportCounts(nodes.get(), rels.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
      throw new IllegalStateException("Parallel export interrupted", e);
    } catch (ExecutionException e) {
      executor.shutdownNow();
      throw new IllegalStateException("Parallel export failed", e.getCause());
    } finally {
      executor.shutdown();
    }
  }

  private Manifest manifest(
      String version,
      String started,
      String finished,
      String status,
      List<String> labels,
      List<String> relTypes,
      List<com.brinqa.neo4j.tool.dto.IndexData> indexes,
      long nodeCount,
      long relCount) {
    final boolean idIndexed =
        indexes.stream()
            .anyMatch(i -> i.getProperties() != null && i.getProperties().contains("id"));
    return Manifest.builder()
        .toolVersion(getClass().getPackage().getImplementationVersion())
        .neo4jVersion(version)
        .startedAt(started)
        .finishedAt(finished)
        .status(status)
        .labels(labels)
        .relationshipTypes(relTypes)
        .nodeCount(nodeCount)
        .relationshipCount(relCount)
        .batchSize(batchSize)
        .formatVersion(Manifest.FORMAT_VERSION)
        .idPropertyPresent(idIndexed)
        .importIdProperty(Manifest.IMPORT_ID)
        .importLabel(Manifest.IMPORT_LABEL)
        .build();
  }

  private record ExportCounts(long nodeCount, long relCount) {}
}
