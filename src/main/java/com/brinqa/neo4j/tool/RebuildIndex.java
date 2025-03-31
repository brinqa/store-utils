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

import com.brinqa.neo4j.tool.dto.IndexData;
import com.brinqa.neo4j.tool.index.IndexManager;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Rebuilds all the indexes one by one.
 *
 * <p>Neo4j will crash from either out of memory or too many files etc if you restore to many
 * indexes at once.
 */
@Command(
        name = "rebuildIndex",
        version = "rebuildIndex 1.0",
        description = "Rebuilds all the indexes one by one.")
public class RebuildIndex extends AbstractIndexCommand {

    @Option(
            defaultValue = "lastIndex",
            names = {"-r", "--resume"},
            description = "File to use for storing the last index.")
    protected File file;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new RebuildIndex()).execute(args);
        System.exit(exitCode);
    }

    @Override
    void execute(final IndexManager indexManager) throws IOException {
        final var indexes = indexManager.readIndexes();
        String lastIndexName = file.isFile() ? Files.readString(file.toPath()) : null;
        for (final IndexData index : indexes) {

            // skip until resume index
            if (StringUtils.isNotBlank(lastIndexName)) {
                // skip until its found
                if (!lastIndexName.equals(index.getName())) {
                    continue;
                }
                // we found it, so set this to null
                lastIndexName = null;
                println("Resuming from index: %s", index.getName());
            }

            // save resume file
            Files.writeString(file.toPath(), index.getName());
            indexManager.createAndMonitor(index, true);
        }
        println("Last index saved to %s", this.file);
    }
}
