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

import com.brinqa.neo4j.tool.dto.IndexData;
import com.brinqa.neo4j.tool.dto.IndexDataComparator;
import com.brinqa.neo4j.tool.index.IndexManager;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
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
    name = "dumpIndex",
    version = "dumpIndex 1.0",
    description =
        "Dumps all the indexes and constraints to a file for 'loadIndex'. The file is newline delimited json.")
public class DumpIndex extends AbstractIndexCommand {

  @Option(
      names = {"-f", "--filename"},
      description = "Name of the file to dump.",
      defaultValue = "dump.jsonl")
  protected File file;

  // this example implements Callable, so parsing, error handling and handling user
  // requests for usage help or version help can be done with one line of code.
  public static void main(String... args) {
    int exitCode = new CommandLine(new DumpIndex()).execute(args);
    System.exit(exitCode);
  }

  @Override
  void execute(final IndexManager indexManager) {
    // query for all the indexes
    final List<IndexData> indexes = indexManager.readIndexes();
    println("Building index file: %s", this.file);
    final List<IndexData> sortedIndexes =
        indexes.stream().sorted(new IndexDataComparator()).collect(Collectors.toList());
    indexManager.writeIndexes(this.file, sortedIndexes);
  }
}
