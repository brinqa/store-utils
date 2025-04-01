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
import com.brinqa.neo4j.tool.index.IndexManager;
import java.io.File;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Takes a dump file and creates each of the constraints and indexes from that file in a controlled
 * manner. In particular, it waits until an index is online before moving to the next as adding too
 * many indexes at any onetime will result in either an OOME or a corrupted index that will need to
 * be refreshed again. Skips existing indexes by default.
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
      defaultValue = "dump.jsonl")
  protected File file;

  @Option(
      names = {"-r", "--refresh"},
      description = "Refresh the index by dropping and recreating.",
      defaultValue = "false")
  protected boolean refresh;

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
        final String query = indexManager.createIndexQueryQuery(x);
        println(query);
      }
      return;
    }

    indexManager.loadIndexes(fileIndexes, refresh);
  }
}
