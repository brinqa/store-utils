/*
 * Copyright 2002 Brinqa, Inc. All rights reserved.
 *
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

import com.brinqa.neo4j.tool.dto.Bucket;
import com.brinqa.neo4j.tool.dto.IndexData;
import com.brinqa.neo4j.tool.index.BucketBuilder;
import com.brinqa.neo4j.tool.index.IndexManager;
import com.google.common.collect.Iterables;
import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Takes a dump file and creates each of the constraints and indexes from that file in a controlled
 * manner. In particular, it waits until an index is online before moving to the next as adding to
 * many indexes at any onetime will result in either an OOME or a corrupted index that will need to
 * be refreshed again.
 */
@Command(
        name = "loadIndex",
        version = "loadIndex 1.0",
        description =
                "Creates indexes and constraints based on the file provided, skips existing indexes and constraints by name.")
public class LoadIndex extends AbstractIndexCommand {

    @Option(
            names = {"-d", "--dryrun"},
            description = "Just print all the queries.")
    protected boolean dryRun;

    @Option(
            required = true,
            names = {"-f", "--filename"},
            description = "File to load all the indexes.",
            defaultValue = "dump.json")
    protected File file;

    @Option(
            names = {"-r", "--recreate"},
            description = "Recreate each of the indexes in the file.")
    protected boolean recreate;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new LoadIndex()).execute(args);
        System.exit(exitCode);
    }

    @Override
    void execute(final IndexManager indexManager) {
        final var fileIndexes = indexManager.readIndexesFromFile(file);

        // just print all the queries
        if (dryRun) {
            for (IndexData x : fileIndexes) {
                final String query = indexManager.indexOrConstraintQuery(x);
                println(query);
            }
            return;
        }

        // buckets sizes <1k (100 per), <10k (10 per), <100k (2 per), >100k (1 per)
        final var indexNames = indexManager.readIndexNames();

        // filter through missing
        final var missing =
                fileIndexes.stream()
                        .filter(indexData -> filterExisting(indexNames, indexData))
                        .collect(Collectors.toList());

        // find all the sizes
        final var sizes =
                missing.parallelStream()
                        .filter(idx -> idx.getLabelsOrTypes().size() == 1)
                        .map(idx -> determineSize(indexManager, idx))
                        .collect(Collectors.toList());

        // process each bucket
        final var buckets = BucketBuilder.build(sizes);
        for (Bucket bucket : buckets) {
            indexManager.create(bucket);
        }
    }

    Pair<IndexData, Long> determineSize(IndexManager indexManager, IndexData idx) {
        String label = Iterables.getFirst(idx.getLabelsOrTypes(), null);
        long size = indexManager.labelSize(label);
        return Pair.of(idx, size);
    }

    boolean filterExisting(Set<String> indexNames, IndexData indexData) {
        if (indexData.getLabelsOrTypes().isEmpty()) {
            println("Filtering as there's no Label: %s", indexData);
            return false;
        }
        // if recreate always drop and then create
        if (!recreate && indexNames.contains(indexData.getName())) {
            println("Index with name '%s' already exists, skipping.", indexData.getName());
            return false;
        }
        return true;
    }
}
