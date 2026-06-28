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

import static com.brinqa.neo4j.tool.dto.IndexData.Type.LOOKUP;
import static com.brinqa.neo4j.tool.util.Print.println;
import static com.brinqa.neo4j.tool.util.Print.progressPercentage;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.stream.Collectors.joining;
import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

import com.brinqa.neo4j.tool.dto.Bucket;
import com.brinqa.neo4j.tool.dto.IndexData;
import com.brinqa.neo4j.tool.dto.IndexStatus;
import com.brinqa.neo4j.tool.dto.IndexStatus.State;
import com.brinqa.neo4j.tool.util.CypherNames;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MoreCollectors;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ResultSummary;

@Slf4j
@AllArgsConstructor
public class IndexManager {

  private final Driver driver;

  static IndexData fromRecord(Record record) {
    // id, name, state, populationPercent
    return IndexData.builder()
        .id(record.get("id").asInt())
        .name(safeToString(record.get("name")))
        .state(safeToString(record.get("state")))
        .populationPercent(record.get("populationPercent").asFloat(0f))
        .type(IndexData.Type.valueOf(safeToString(record.get("type"))))
        .entityType(record.get("entityType").asString())
        .labelsOrTypes(toList(record.get("labelsOrTypes")))
        .properties(toList(record.get("properties")))
        .indexProvider(safeToString(record.get("indexProvider")))
        .owningConstraint(safeToString(record.get("owningConstraint")))
        .options(
            record.containsKey("options") ? toMap(record.get("options")) : new LinkedHashMap<>())
        .readCount(record.get("readCount").asLong(0))
        .build();
  }

  static String safeToString(Value value) {
    if (value == null || value.isNull()) {
      return null;
    }
    final var ret = value.asString(null);
    if ("null".equals(ret)) {
      return null;
    }
    return ret;
  }

  static List<String> toList(Value value) {
    if (value == null || value.isNull() || value.isEmpty()) {
      return List.of();
    }
    if (TYPE_SYSTEM.STRING().equals(value.type())) {
      return List.of(safeToString(value));
    }
    return value.asList(IndexManager::safeToString);
  }

  @SuppressWarnings("unchecked")
  static LinkedHashMap<String, Object> toMap(Value value) {
    if (value == null || value.isNull() || value.isEmpty()) {
      return new LinkedHashMap<>();
    }
    return new LinkedHashMap<>((java.util.Map<String, Object>) value.asObject());
  }

  public List<IndexData> readIndexesFromFile(File f) {
    final var ret = new ArrayList<IndexData>();
    final var objectMapper = new ObjectMapper();
    try (final var rdr = Files.newBufferedReader(f.toPath())) {
      for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
        final var index = objectMapper.readValue(line, IndexData.class);
        ret.add(index);
      }
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
    return ret;
  }

  public void createIndex(IndexData index) {
    final var query = createIndexQueryQuery(index);
    println(query);
    try {
      writeTransaction(query);
    } catch (Throwable th) {
      log.error("Failed to create index: {}", query, th);
      throw new RuntimeException(th);
    }
    if (!validIndex(index)) {
      println("Failed to create index: %s", index.getName());
    }
  }

  boolean validIndex(final IndexData index) {
    // insure index creation started
    for (int i = 0; i < 10; i++) {
      final var status = readIndexStatus(index.getName());
      if (null != status && status.getState().isOk()) {
        return true;
      }
      simpleWait(10);
    }
    return false;
  }

  private void simpleWait(long milliseconds) {
    try {
      TimeUnit.MILLISECONDS.sleep(milliseconds);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void monitorCreation(final IndexData index) {

    // wait for completion
    int pct = 0;
    IndexStatus status = null;
    while (pct < 100) {
      progressPercentage(pct);
      status = readIndexStatus(index.getName());
      if (status.getState().isFailed()) {
        println("%nFailed to create index: %s", index.getName());
        return;
      }
      pct = (int) status.getProgress();
      progressPercentage(pct);
      if (pct != 100) {
        simpleWait(1000);
      }
    }
    if (status.getState().isOk()) {
      return;
    }
    final var ERROR_FMT = "Index '%s' failed to come online, please create manually.";
    println(String.format(ERROR_FMT, index.getName()));
  }

  void writeTransaction(final String query) {
    // query for all the indexes
    try (final Session session = driver.session()) {
      assert session != null;
      ResultSummary resultSummary = session.executeWrite(tx -> tx.run(query).consume());
      if (log.isDebugEnabled()) {
        log.debug(resultSummary.toString());
      }
    }
  }

  <T> T readTransaction(TransactionCallback<T> work) {
    // query for all the indexes
    final var cfg = SessionConfig.builder().withDefaultAccessMode(AccessMode.READ).build();
    try (final Session session = driver.session(cfg)) {
      assert session != null;
      return session.executeRead(work);
    }
  }

  public String createIndexQueryQuery(IndexData idx) {
    return switch (idx.getType()) {
      case RANGE, TEXT, POINT -> buildIndexQuery(idx);
      case VECTOR -> buildVectorIndexQuery(idx);
      case FULLTEXT -> buildFullTextIndexQuery(idx);
      case LOOKUP -> buildLookupQuery(idx);
    };
  }

  private String buildLookupQuery(IndexData idx) {
    final var name = CypherNames.quote(idx.getName());
    final var NODE_LOOKUP = "CREATE LOOKUP INDEX %s IF NOT EXISTS FOR (n) ON EACH labels(n);";
    if ("NODE".equals(idx.getEntityType())) {
      return String.format(NODE_LOOKUP, name);
    }
    final var REL_LOOKUP = "CREATE LOOKUP INDEX %s IF NOT EXISTS FOR ()-[r]-() ON EACH type(r);";
    if ("RELATIONSHIP".equals(idx.getEntityType())) {
      return String.format(REL_LOOKUP, name);
    }
    throw new IllegalStateException("Unsupported index entity type: " + idx.getEntityType());
  }

  /** Full text indexes, for nodes or relationships. */
  String buildFullTextIndexQuery(IndexData indexData) {
    final var name = CypherNames.quote(indexData.getName());
    // a fulltext index may span multiple labels/types: :`A`|`B`
    final var tokens =
        indexData.getLabelsOrTypes().stream().map(CypherNames::quote).collect(joining("|"));
    final var rel = "RELATIONSHIP".equals(indexData.getEntityType());
    final var entity = rel ? "()-[n:" + tokens + "]-()" : "(n:" + tokens + ")";
    // make sure to quote all the properties of an index
    final var properties =
        indexData.getProperties().stream()
            .map(p -> "n." + CypherNames.quote(p))
            .collect(joining(","));
    final var fmt = "CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR %s ON EACH [%s]%s;";
    return String.format(fmt, name, entity, properties, options(indexData));
  }

  String buildIndexQuery(IndexData indexData) {
    if (indexData.getOwningConstraint() != null) {
      return buildConstraintQuery(indexData);
    }
    // basic name/label
    var name = CypherNames.quote(indexData.getName());
    var label =
        CypherNames.quote(
            indexData.getLabelsOrTypes().stream().collect(MoreCollectors.onlyElement()));
    final var rel = "RELATIONSHIP".equals(indexData.getEntityType());
    final var entity = rel ? "()-[n:" + label + "]-()" : "(n:" + label + ")";

    // create an index
    var IDX_FMT = "CREATE %s INDEX %s IF NOT EXISTS FOR %s ON (%s)%s;";
    // make sure to quote all the properties of an index
    var properties =
        indexData.getProperties().stream()
            .map(p -> "n." + CypherNames.quote(p))
            .collect(joining(","));
    return String.format(
        IDX_FMT, indexData.getType(), name, entity, properties, options(indexData));
  }

  String buildVectorIndexQuery(IndexData indexData) {
    if (indexData.getOwningConstraint() != null) {
      throw new IllegalStateException(
          "VECTOR indexes cannot own constraints: " + indexData.getName());
    }
    final var name = CypherNames.quote(indexData.getName());
    final var label =
        CypherNames.quote(
            indexData.getLabelsOrTypes().stream().collect(MoreCollectors.onlyElement()));
    final var rel = "RELATIONSHIP".equals(indexData.getEntityType());
    final var entity = rel ? "()-[n:" + label + "]-()" : "(n:" + label + ")";
    if (indexData.getProperties() == null || indexData.getProperties().isEmpty()) {
      throw new IllegalStateException("VECTOR index has no property: " + indexData.getName());
    }
    final var vectorProperty = "n." + CypherNames.quote(indexData.getProperties().get(0));
    final var filterProperties =
        indexData.getProperties().stream()
            .skip(1)
            .map(p -> "n." + CypherNames.quote(p))
            .collect(joining(","));
    final var filters = filterProperties.isEmpty() ? "" : " WITH [" + filterProperties + "]";
    return String.format(
        "CREATE VECTOR INDEX %s IF NOT EXISTS FOR %s ON (%s)%s%s;",
        name, entity, vectorProperty, filters, options(indexData));
  }

  private static String options(IndexData indexData) {
    final var options = indexData.getOptions();
    if (options == null || options.isEmpty()) {
      return "";
    }
    return " OPTIONS " + cypherLiteral(options);
  }

  private static String cypherLiteral(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof String s) {
      return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
    if (value instanceof Number || value instanceof Boolean) {
      return String.valueOf(value);
    }
    if (value instanceof List<?> list) {
      return "[" + list.stream().map(IndexManager::cypherLiteral).collect(joining(",")) + "]";
    }
    if (value instanceof java.util.Map<?, ?> map) {
      return "{"
          + map.entrySet().stream()
              .map(
                  e ->
                      CypherNames.quote(String.valueOf(e.getKey()))
                          + ": "
                          + cypherLiteral(e.getValue()))
              .collect(joining(","))
          + "}";
    }
    throw new IllegalArgumentException("Unsupported index option value: " + value.getClass());
  }

  String buildConstraintQuery(IndexData indexData) {
    var name = CypherNames.quote(indexData.getName());
    var labels = CypherNames.quote(getOnlyElement(indexData.getLabelsOrTypes()));

    // create constraint
    var format = "CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:%s) REQUIRE (%s) IS UNIQUE;";
    var properties =
        indexData.getProperties().stream()
            .map(p -> "n." + CypherNames.quote(p))
            .collect(joining(","));
    return String.format(format, name, labels, properties);
  }

  IndexStatus readIndexStatus(String name) {
    // query for all the indexes
    try (final Session session = driver.session()) {
      assert session != null;
      return session.executeRead(tx -> readIndexStatus(tx, name));
    }
  }

  IndexStatus readIndexStatus(TransactionContext tx, String name) {
    final String FMT = "show indexes yield populationPercent,state,name WHERE name = \"%s\"";
    final var result = tx.run(String.format(FMT, name));
    return result.list().stream()
        .findFirst()
        .map(
            record -> {
              final var pct = record.get("populationPercent").asFloat(0f);
              final var state = record.get("state").asString("");
              return IndexStatus.builder().progress(pct).state(toState(state)).build();
            })
        .orElseGet(
            () -> {
              log.error("Unable to get index status for {}", name);
              return IndexStatus.builder().state(State.FAILED).build();
            });
  }

  State toState(String state) {
    return switch (state.toUpperCase()) {
      case "ONLINE" -> State.ONLINE;
      case "FAILED" -> State.FAILED;
      case "POPULATING" -> State.POPULATING;

      default -> State.OTHER;
    };
  }

  public void writeIndexes(File file, List<IndexData> indexes) {
    final var objectMapper = new ObjectMapper();
    try (final var wrt = Files.newBufferedWriter(file.toPath(), TRUNCATE_EXISTING, CREATE)) {
      for (IndexData index : indexes) {
        final var line = objectMapper.writeValueAsString(index);
        wrt.write(line);
        wrt.newLine();
      }
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
  }

  /** Read all the index and constraints in order, of constrains first. */
  public List<IndexData> readIndexes() {
    try (Session session = driver.session()) {
      assert session != null;
      return session.executeRead(
          tx ->
              tx.run("show indexes;").list().stream()
                  .map(IndexManager::fromRecord)
                  .collect(Collectors.toList()));
    }
  }

  static String dropQuery(final IndexData idx) {
    final boolean constraint = idx.getOwningConstraint() != null;
    final var FMT = (constraint ? "DROP CONSTRAINT %s" : "DROP INDEX %s") + " IF EXISTS;";
    return String.format(FMT, idx.getName());
  }

  public void dropIndex(final IndexData indexData) {
    // query for all the indexes
    final var query = dropQuery(indexData);
    println(query);
    try {
      writeTransaction(query);

    } catch (Throwable th) {
      log.error("Failed to drop index: {}", query, th);
    }
  }

  public long labelSize(String labelName) {
    final String FMT = "MATCH (n:`%s`) return count(n) as count";
    return this.readTransaction(
        tx -> {
          final Result result = tx.run(String.format(FMT, labelName));
          return result.list().stream().findFirst().map(r -> r.get(0).asLong(0L)).orElse(0L);
        });
  }

  /** Create all the indexes for the bucket, then wait for each to come online. */
  void create(final Bucket bucket, final boolean recreate, int current, int total) {
    // send all the commands
    final var indexCount = bucket.getIndexes().size();
    println("Creating %d indexes for bucket size: %s", indexCount, bucket.getSize());
    // Create sequentially: the README documents that creating too many indexes at once can OOM or
    // corrupt Neo4j, so each index is created (after an optional drop) before the next.
    for (IndexData index : bucket.getIndexes()) {
      if (recreate) {
        // drop the index if it exists
        dropIndex(index);
      }
      // create the index if it does not exist
      createIndex(index);
    }
    // monitor the indexes
    for (int i = 0; i < bucket.getIndexes().size(); i++) {
      final var idxCount = current + i + 1;
      final var idxData = bucket.getIndexes().get(i);
      println("Monitoring: %s, %d of %d", idxData.getName(), idxCount, total);
      monitorCreation(idxData);
    }
  }

  Pair<IndexData, Long> determineSize(IndexData idx) {
    return idx.getLabelsOrTypes().stream()
        .findFirst()
        .map(
            label -> {
              long size = labelSize(label);
              return Pair.of(idx, size);
            })
        .orElse(Pair.of(idx, 0L));
  }

  /**
   * Method called by LoadIndex
   *
   * @param indexes indexes to create/refresh
   * @param refresh if the index should be 'dropped' first
   */
  public void loadIndexes(List<IndexData> indexes, boolean refresh) {

    // bucket the FULLTEXT/TEXT/RANGE indexes
    final var index2Size =
        indexes.parallelStream()
            // FIXME: If there's multiple labels on FULLTEXT
            .filter(idx -> idx.getLabelsOrTypes().size() == 1)
            .map(this::determineSize)
            .toList();

    // buckets sizes <1k (100 per), <10k (10 per), <100k (2 per), >100k (1 per)
    AtomicInteger currentCount = new AtomicInteger(0);
    BucketBuilder.build(index2Size)
        .forEach(
            b -> {
              create(b, refresh, currentCount.get(), indexes.size());
              currentCount.addAndGet(b.getIndexes().size());
            });

    // SKIP IndexType.LOOKUP as Neo4j will create it automatically
  }
}
