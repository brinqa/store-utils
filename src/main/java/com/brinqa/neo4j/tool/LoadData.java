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

import com.brinqa.neo4j.tool.data.Importer;
import com.brinqa.neo4j.tool.data.Manifest;
import com.brinqa.neo4j.tool.data.StagingStore;
import com.brinqa.neo4j.tool.index.IndexManager;
import java.io.File;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Imports a dump produced by {@link DumpData} into a clean, running Neo4j 5 database: nodes first,
 * then relationships, then (optionally) the indexes and constraints from the dump.
 */
@Command(
    name = "loadData",
    version = "loadData 1.0",
    description = "Import nodes, relationships and indexes from a staging directory.")
public class LoadData implements Runnable {

  @Mixin ConnectionOptions conn;

  @Option(
      names = {"-i", "--input"},
      description = "Staging directory produced by dumpData.",
      defaultValue = "dump")
  File input;

  @Option(
      names = {"-b", "--batch"},
      description = "Batch size used for UNWIND CREATE.",
      defaultValue = "10000")
  int batchSize;

  @Option(
      names = {"--keep-import-keys"},
      description = "Keep the temporary import label/property (for debugging or resume).")
  boolean keepImportKeys;

  @Option(
      names = {"--skip-indexes"},
      description = "Do not recreate indexes/constraints from indexes.jsonl.")
  boolean skipIndexes;

  @Option(
      names = {"-p", "--parallelism"},
      description = "Number of staged node/relationship partitions to import concurrently.",
      defaultValue = "1")
  int parallelism;

  public static void main(String... args) {
    System.exit(new CommandLine(new LoadData()).execute(args));
  }

  @Override
  public void run() {
    final var manifest = Manifest.read(input);
    if (!"COMPLETE".equals(manifest.getStatus())) {
      throw new IllegalStateException(
          "Refusing to load: dump status is '" + manifest.getStatus() + "', not COMPLETE.");
    }
    if (manifest.getFormatVersion() != Manifest.FORMAT_VERSION) {
      throw new IllegalStateException(
          "Dump format version "
              + manifest.getFormatVersion()
              + " is not supported (expected "
              + Manifest.FORMAT_VERSION
              + ").");
    }

    try (final var driver = conn.buildDriver()) {
      final var store = new StagingStore(input);
      final var importer = new Importer(driver, store, batchSize, keepImportKeys, parallelism);

      println("Importing nodes into %s", conn.uri);
      final long nodes = importer.importNodes();
      println("Imported %d nodes (manifest reported %d)", nodes, manifest.getNodeCount());

      println("Importing relationships");
      final long rels = importer.importRelationships();
      println(
          "Imported %d relationships (manifest reported %d)",
          rels, manifest.getRelationshipCount());

      final var indexFile = new File(input, "indexes.jsonl");
      if (!skipIndexes && indexFile.isFile()) {
        println("Recreating indexes and constraints from %s", indexFile);
        final var indexManager = new IndexManager(driver);
        indexManager.loadIndexes(indexManager.readIndexesFromFile(indexFile), false);
      }
      println("Load complete.");
    }
  }
}
