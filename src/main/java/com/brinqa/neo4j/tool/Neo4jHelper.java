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

import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

@Slf4j
public final class Neo4jHelper {
  private Neo4jHelper() {}

  public static Driver buildDriver(String uri, String username, String password, boolean noAuth) {
    // create the driver
    for (int i = 0; i < 5; i++) {
      try {
        final var config = Config.defaultConfig();
        if (noAuth) {
          println("Attempting to connect without authentication.");
          return GraphDatabase.driver(uri, config);
        }
        println("Attempting to connect with basic authentication.");
        final var token = AuthTokens.basic(username, password);
        return GraphDatabase.driver(uri, token, config);
      } catch (ServiceUnavailableException ex) {
        log.error("Failed to connect retrying..");
      }
    }
    throw new IllegalStateException("Unable to connect to Neo4J: " + uri);
  }
}
