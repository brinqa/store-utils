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

import java.io.File;
import java.util.List;
import com.brinqa.neo4j.tool.dto.IndexData;
import com.brinqa.neo4j.tool.index.IndexManager;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Dumps all the current indexes to a file for loading indexes.
 *
 * <p>Neo4j will crash from either out of memory or too many files etc if you restore to many
 * indexes at once.
 */
@Command(
        name = "dropIndex",
        version = "dropIndex 1.0",
        description = "Drops indexes based on the dump file.")
public class DropIndex extends AbstractIndexCommand {

    @Option(
            required = true,
            names = {"-f", "--file"},
            description = "File to use to drop indexes.")
    protected File file;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new DropIndex()).execute(args);
        System.exit(exitCode);
    }

    @Override
    void execute(final IndexManager indexManager) {
        // query for all the indexes
        final List<IndexData> indexes = indexManager.readIndexesFromFile(this.file);
        println("Dropping indexes from file: %s", this.file.getAbsoluteFile());
        for (IndexData index : indexes) {
            println("Dropping %s", index.getName());
            indexManager.dropIndex(index);
        }
    }
}
