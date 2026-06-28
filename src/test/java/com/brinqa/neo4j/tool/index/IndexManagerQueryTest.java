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
package com.brinqa.neo4j.tool.index;

import static org.junit.Assert.assertEquals;

import com.brinqa.neo4j.tool.dto.IndexData;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Pure string generation — no driver needed, so {@code new IndexManager(null)} is fine. */
public class IndexManagerQueryTest {

  private final IndexManager mgr = new IndexManager(null);

  private static IndexData.IndexDataBuilder idx() {
    return IndexData.builder().entityType("NODE");
  }

  @Test
  public void rangeIndexEscapesEverything() {
    final var q =
        mgr.createIndexQueryQuery(
            idx()
                .name("my idx")
                .type(IndexData.Type.RANGE)
                .labelsOrTypes(List.of("Lab`el"))
                .properties(List.of("pro p"))
                .build());
    // backtick inside the label is doubled; spaces are safely quoted
    assertEquals("CREATE RANGE INDEX `my idx` IF NOT EXISTS FOR (n:`Lab``el`) ON (n.`pro p`);", q);
  }

  @Test
  public void constraintEscapesEverything() {
    final var q =
        mgr.createIndexQueryQuery(
            idx()
                .name("c")
                .type(IndexData.Type.RANGE)
                .owningConstraint("c")
                .labelsOrTypes(List.of("Person"))
                .properties(List.of("name"))
                .build());
    assertEquals(
        "CREATE CONSTRAINT `c` IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`name`) IS UNIQUE;", q);
  }

  @Test
  public void rangeIndexOnRelationshipUsesRelPattern() {
    final var q =
        mgr.createIndexQueryQuery(
            idx()
                .name("rel idx")
                .type(IndexData.Type.RANGE)
                .entityType("RELATIONSHIP")
                .labelsOrTypes(List.of("KNOWS"))
                .properties(List.of("since"))
                .build());
    assertEquals(
        "CREATE RANGE INDEX `rel idx` IF NOT EXISTS FOR ()-[n:`KNOWS`]-() ON (n.`since`);", q);
  }

  @Test
  public void fulltextNodeQuotesProperties() {
    final var q =
        mgr.createIndexQueryQuery(
            idx()
                .name("ft")
                .type(IndexData.Type.FULLTEXT)
                .labelsOrTypes(List.of("A", "B"))
                .properties(List.of("p1", "p2"))
                .build());
    assertEquals(
        "CREATE FULLTEXT INDEX `ft` IF NOT EXISTS FOR (n:`A`|`B`) ON EACH [n.`p1`,n.`p2`];", q);
  }

  @Test
  public void fulltextRelationshipUsesRelPattern() {
    final var q =
        mgr.createIndexQueryQuery(
            idx()
                .name("ftr")
                .type(IndexData.Type.FULLTEXT)
                .entityType("RELATIONSHIP")
                .labelsOrTypes(List.of("KNOWS"))
                .properties(List.of("note"))
                .build());
    assertEquals(
        "CREATE FULLTEXT INDEX `ftr` IF NOT EXISTS FOR ()-[n:`KNOWS`]-() ON EACH [n.`note`];", q);
  }

  @Test
  public void relationshipLookupHasIfNotExists() {
    final var q =
        mgr.createIndexQueryQuery(
            idx().name("rl").type(IndexData.Type.LOOKUP).entityType("RELATIONSHIP").build());
    assertEquals("CREATE LOOKUP INDEX `rl` IF NOT EXISTS FOR ()-[r]-() ON EACH type(r);", q);
  }

  @Test
  public void pointIndexUsesNeo4j5Syntax() {
    final var q =
        mgr.createIndexQueryQuery(
            idx()
                .name("point idx")
                .type(IndexData.Type.POINT)
                .labelsOrTypes(List.of("Place"))
                .properties(List.of("location"))
                .build());
    assertEquals(
        "CREATE POINT INDEX `point idx` IF NOT EXISTS FOR (n:`Place`) ON (n.`location`);", q);
  }

  @Test
  public void vectorIndexKeepsOptions() {
    final var indexConfig = new LinkedHashMap<String, Object>();
    indexConfig.put("vector.dimensions", 1536L);
    indexConfig.put("vector.similarity_function", "cosine");
    final Map<String, Object> options = new LinkedHashMap<>();
    options.put("indexConfig", indexConfig);

    final var q =
        mgr.createIndexQueryQuery(
            idx()
                .name("embedding")
                .type(IndexData.Type.VECTOR)
                .labelsOrTypes(List.of("Document"))
                .properties(List.of("embedding"))
                .options(options)
                .build());
    assertEquals(
        "CREATE VECTOR INDEX `embedding` IF NOT EXISTS FOR (n:`Document`) ON (n.`embedding`)"
            + " OPTIONS {`indexConfig`: {`vector.dimensions`: 1536,`vector.similarity_function`: 'cosine'}};",
        q);
  }
}
