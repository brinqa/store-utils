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
package com.brinqa.neo4j.tool.data;

import static com.brinqa.neo4j.tool.util.Print.println;

import java.util.LinkedHashMap;
import java.util.Map;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;

/**
 * Streams all nodes and relationships out of a running Neo4j 5 database into a {@link
 * StagingStore}.
 *
 * <p>The export is a single streamed scan per entity (driver fetch size bounds memory), so it never
 * materializes the graph. The element id is used as the export-local key; it is only a cursor
 * within this one export and is rewritten to a temporary import-id property on load.
 */
public class Exporter {

  private static final String NODE_QUERY =
      "MATCH (n) RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props";

  private static final String REL_QUERY =
      "MATCH (s)-[r]->(t) RETURN elementId(r) AS id, elementId(s) AS src,"
          + " elementId(t) AS dst, type(r) AS type, properties(r) AS props";

  private final Driver driver;
  private final StagingStore store;
  private final int batchSize;

  public Exporter(Driver driver, StagingStore store, int batchSize) {
    this.driver = driver;
    this.store = store;
    this.batchSize = batchSize;
  }

  public long exportNodes() {
    try (Session session = streamingSession();
        StagingStore.NodeWriter writer = store.newNodeWriter()) {
      return session.executeRead(
          tx -> {
            final Result result = tx.run(NODE_QUERY);
            long count = 0;
            while (result.hasNext()) {
              final var rec = result.next();
              writer.add(
                  new NodeRecord(
                      rec.get("id").asString(),
                      rec.get("labels").asList(Value::asString),
                      normalize(rec.get("props").asMap())));
              if (++count % batchSize == 0) {
                println("  exported %d nodes", count);
              }
            }
            return count;
          });
    }
  }

  public long exportRelationships() {
    try (Session session = streamingSession();
        StagingStore.RelWriter writer = store.newRelWriter()) {
      return session.executeRead(
          tx -> {
            final Result result = tx.run(REL_QUERY);
            long count = 0;
            while (result.hasNext()) {
              final var rec = result.next();
              writer.add(
                  new RelRecord(
                      rec.get("src").asString(),
                      rec.get("dst").asString(),
                      rec.get("type").asString(),
                      normalize(rec.get("props").asMap())));
              if (++count % batchSize == 0) {
                println("  exported %d relationships", count);
              }
            }
            return count;
          });
    }
  }

  private Session streamingSession() {
    return driver.session(SessionConfig.builder().withFetchSize(batchSize).build());
  }

  private static Map<String, Object> normalize(Map<String, Object> props) {
    final var out = new LinkedHashMap<String, Object>(props.size());
    props.forEach((k, v) -> out.put(k, PropertyCodec.normalize(v)));
    return out;
  }
}
