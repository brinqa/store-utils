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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.brinqa.neo4j.tool.data.Manifest;
import com.brinqa.neo4j.tool.util.Neo4jHelper;
import java.io.File;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Value;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

/** End-to-end dumpData -&gt; wipe -&gt; loadData round trip against a real Neo4j 5 container. */
public class DataRoundTripTest {

  private static Neo4jContainer<?> neo4j;
  private static Driver driver;

  @BeforeClass
  public static void init() {
    Assume.assumeTrue(
        "Docker is required for DataRoundTripTest",
        DockerClientFactory.instance().isDockerAvailable());
    neo4j = new Neo4jContainer<>(DockerImageName.parse("neo4j:5.26"));
    neo4j.start();
    driver = Neo4jHelper.buildDriver(neo4j.getBoltUrl(), "neo4j", neo4j.getAdminPassword(), false);
  }

  @AfterClass
  public static void close() {
    if (driver != null) {
      driver.close();
    }
    if (neo4j != null) {
      neo4j.stop();
    }
  }

  @Test
  public void roundTrip() throws Exception {
    seed();

    final long expectedNodes = count("MATCH (n) RETURN count(n) AS c");
    final long expectedRels = count("MATCH ()-[r]->() RETURN count(r) AS c");
    assertEquals(3, expectedNodes);
    assertEquals(2, expectedRels);

    final File dumpDir = Files.createTempDirectory("store-utils-dump").toFile();

    // --- dump ---
    final var dump = new DumpData();
    dump.conn = connection();
    dump.output = dumpDir;
    dump.batchSize = 100;
    dump.run();

    final var manifest = Manifest.read(dumpDir);
    assertEquals("COMPLETE", manifest.getStatus());
    assertEquals(expectedNodes, manifest.getNodeCount());
    assertEquals(expectedRels, manifest.getRelationshipCount());
    assertTrue(manifest.getLabels().contains("Person"));
    assertTrue(manifest.getRelationshipTypes().contains("KNOWS"));

    // --- wipe target clean ---
    wipe();
    assertEquals(0, count("MATCH (n) RETURN count(n) AS c"));

    // --- load ---
    final var load = new LoadData();
    load.conn = connection();
    load.input = dumpDir;
    load.batchSize = 100;
    load.run();

    // --- verify structure ---
    assertEquals(expectedNodes, count("MATCH (n) RETURN count(n) AS c"));
    assertEquals(expectedRels, count("MATCH ()-[r]->() RETURN count(r) AS c"));
    assertEquals(1, count("MATCH (n:Person:Admin) RETURN count(n) AS c"));
    assertEquals(
        1,
        count(
            "MATCH (:Person {name:'alice'})-[:KNOWS]->(:Person {name:'bob'}) RETURN count(*) AS c"));
    // direction is preserved: there is no reverse KNOWS
    assertEquals(
        0,
        count(
            "MATCH (:Person {name:'bob'})-[:KNOWS]->(:Person {name:'alice'}) RETURN count(*) AS c"));
    assertEquals(
        1,
        count(
            "MATCH (:Person {name:'bob'})-[:LIKES]->(:Company {name:'acme'}) RETURN count(*) AS c"));

    // --- verify property fidelity on alice ---
    final var alice = readNode("MATCH (n:Person {name:'alice'}) RETURN n AS n");
    assertEquals("alice", alice.get("name").asString());
    assertEquals(30L, alice.get("age").asLong());
    assertEquals(1.5, alice.get("score").asDouble(), 0.0);
    assertTrue(alice.get("active").asBoolean());
    assertEquals(List.of("x", "y"), alice.get("tags").asList(Value::asString));
    assertEquals(List.of(1L, 2L, 3L), alice.get("nums").asList(Value::asLong));
    assertEquals(LocalDate.of(2020, 1, 2), alice.get("born").asLocalDate());
    assertEquals(
        ZonedDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC),
        alice.get("seen").asZonedDateTime());
    assertEquals(1.0, alice.get("loc").asPoint().x(), 0.0);
    assertEquals(2.0, alice.get("loc").asPoint().y(), 0.0);

    // --- verify temporary import keys were removed ---
    assertEquals(0, count("MATCH (n:`__Imported`) RETURN count(n) AS c"));
    assertEquals(0, count("MATCH (n) WHERE n.`__import_id` IS NOT NULL RETURN count(n) AS c"));

    // --- verify schema was recreated ---
    assertEquals(
        1, count("SHOW CONSTRAINTS YIELD name WHERE name = 'person_name' RETURN count(*) AS c"));
    assertEquals(
        1, count("SHOW INDEXES YIELD name WHERE name = 'company_id' RETURN count(*) AS c"));
  }

  private void seed() {
    write("CREATE CONSTRAINT person_name IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS UNIQUE");
    write("CREATE INDEX company_id IF NOT EXISTS FOR (n:Company) ON (n.id)");
    write(
        "CREATE (alice:Person:Admin {name:'alice', age:30, score:1.5, active:true,"
            + " tags:['x','y'], nums:[1,2,3], born: date('2020-01-02'),"
            + " seen: datetime('2020-01-02T03:04:05Z'), loc: point({x:1.0, y:2.0})}),"
            + " (bob:Person {name:'bob'}),"
            + " (acme:Company {name:'acme', id:'c-1'}),"
            + " (alice)-[:KNOWS {since:2020}]->(bob),"
            + " (bob)-[:LIKES {strength:0.9}]->(acme)");
  }

  private void wipe() {
    forEachName(
        "SHOW CONSTRAINTS YIELD name RETURN name", n -> write("DROP CONSTRAINT `" + n + "`"));
    forEachName(
        "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name",
        n -> write("DROP INDEX `" + n + "`"));
    write("MATCH (n) DETACH DELETE n");
  }

  private static ConnectionOptions connection() {
    final var conn = new ConnectionOptions();
    conn.uri = neo4j.getBoltUrl();
    conn.username = "neo4j";
    conn.password = neo4j.getAdminPassword();
    conn.noAuth = false;
    return conn;
  }

  private void write(String cypher) {
    try (var s = driver.session()) {
      s.executeWrite(tx -> tx.run(cypher).consume());
    }
  }

  private long count(String cypher) {
    try (var s = driver.session()) {
      return s.executeWrite(tx -> tx.run(cypher).single().get("c").asLong());
    }
  }

  private org.neo4j.driver.types.Node readNode(String cypher) {
    try (var s = driver.session()) {
      return s.executeRead(tx -> tx.run(cypher).single().get("n").asNode());
    }
  }

  private void forEachName(String query, java.util.function.Consumer<String> action) {
    final List<String> names;
    try (var s = driver.session()) {
      names =
          s.executeRead(
              tx -> tx.run(query).list().stream().map(r -> r.get("name").asString()).toList());
    }
    names.forEach(action);
  }
}
