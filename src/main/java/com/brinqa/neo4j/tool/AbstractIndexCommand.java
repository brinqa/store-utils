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

import static com.brinqa.neo4j.tool.util.Neo4jHelper.buildDriver;

import com.brinqa.neo4j.tool.index.IndexManager;
import java.io.IOException;
import picocli.CommandLine.Option;

abstract class AbstractIndexCommand implements Runnable {

  @Option(
      names = {"-n", "--no_auth"},
      description = "No authentication.")
  protected boolean noAuth;

  @Option(
      names = {"-a", "--url"},
      description = "Neo4j URL",
      defaultValue = "${NEO4J_URL:-bolt://localhost:7687}")
  protected String uri;

  @Option(
      names = {"-u", "--username"},
      description = "Neo4j Username",
      defaultValue = "${NEO4J_USERNAME}")
  protected String username;

  @Option(
      names = {"-p", "--password"},
      description = "Neo4j Password",
      defaultValue = "${NEO4J_PASSWORD}")
  protected String password;

  @Override
  public void run() {
    try (final var driver = buildDriver(uri, username, password, noAuth)) {
      execute(new IndexManager(driver));
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
  }

  abstract void execute(IndexManager manager) throws IOException;
}
