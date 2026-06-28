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

import com.brinqa.neo4j.tool.util.Neo4jHelper;
import org.neo4j.driver.Driver;
import picocli.CommandLine.Option;

/** Shared Neo4j connection options, mixed into the data commands. */
public class ConnectionOptions {

  @Option(
      names = {"-n", "--no_auth"},
      description = "No authentication.")
  boolean noAuth;

  @Option(
      names = {"-a", "--url"},
      description = "Neo4j URL",
      defaultValue = "${NEO4J_URL:-bolt://localhost:7687}")
  String uri;

  @Option(
      names = {"-u", "--username"},
      description = "Neo4j Username",
      defaultValue = "${NEO4J_USERNAME}")
  String username;

  @Option(
      names = {"-p", "--password"},
      description = "Neo4j Password",
      defaultValue = "${NEO4J_PASSWORD}")
  String password;

  public Driver buildDriver() {
    return Neo4jHelper.buildDriver(uri, username, password, noAuth);
  }
}
