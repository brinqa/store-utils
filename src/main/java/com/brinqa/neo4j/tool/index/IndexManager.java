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
package com.brinqa.neo4j.tool.index;

import static com.brinqa.neo4j.tool.util.Print.println;
import static com.brinqa.neo4j.tool.util.Print.progressPercentage;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

import com.brinqa.neo4j.tool.dto.Bucket;
import com.brinqa.neo4j.tool.dto.IndexBatch;
import com.brinqa.neo4j.tool.dto.IndexData;
import com.brinqa.neo4j.tool.dto.IndexStatus;
import com.brinqa.neo4j.tool.dto.IndexStatus.State;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
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

  public List<IndexData> readIndexesFromFile(File f) {
    final var ret = new ArrayList<IndexData>();
    final var objectMapper = new ObjectMapper();
    try (final var rdr = new BufferedReader(new FileReader(f))) {
      for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
        final var index = objectMapper.readValue(line, IndexData.class);
        ret.add(index);
      }
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
    return ret;
  }

  void createIndex(IndexData index) {
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

  void monitorCreation(final IndexData index) {
    println("Monitoring: %s", index.getName());
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
      simpleWait(100);
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
    switch (idx.getType()) {
      case RANGE:
      case TEXT:
        return buildIndexQuery(idx);
      case FULLTEXT:
        return buildFullTextIndexQuery(idx);
      case LOOKUP:
        return buildIndexQuery(idx);
    }

    throw new IllegalStateException("Unsupported index type: " + idx.getType());
  }

  /** Limited support for the Full Text. */
  String buildFullTextIndexQuery(IndexData indexData) {

    var name = indexData.getName();
    var label = indexData.getLabelsOrTypes().stream().map(l -> "`" + l + "`").collect(joining("|"));

    // create an index
    var fmt = "CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR (n:%s) ON EACH [%s];";
    // make sure to quote all the properties of an index
    var properties = indexData.getProperties().stream().map(p -> "n." + p).collect(joining(","));
    return String.format(fmt, name, label, properties);
  }

  String buildIndexQuery(IndexData indexData) {
    if (indexData.getOwningConstraint() != null) {
      return buildConstraintQuery(indexData);
    }
    // basic name/label
    var name = indexData.getName();
    var label = getOnlyElement(indexData.getLabelsOrTypes());

    // create an index
    var IDX_FMT = "CREATE %s INDEX %s IF NOT EXISTS FOR (n:`%s`) ON (%s);";
    // make sure to quote all the properties of an index
    var properties =
        indexData.getProperties().stream().map(p -> "n.`" + p + "`").collect(joining(","));
    return String.format(IDX_FMT, indexData.getType(), name, label, properties);
  }

  String buildConstraintQuery(IndexData indexData) {
    var name = indexData.getName();
    var labels = getOnlyElement(indexData.getLabelsOrTypes());

    // create constraint
    var format = "CREATE CONSTRAINT `%s` IF NOT EXISTS FOR (n:`%s`) REQUIRE (%s) IS UNIQUE;";
    var properties =
        indexData.getProperties().stream().map(p -> "n.`" + p + "`").collect(joining(","));
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
    final var record = Iterables.getFirst(result.list(), null);
    if (null == record) {
      log.error("Unable to get index status for {}", name);
      return IndexStatus.builder().state(State.FAILED).build();
    }
    final var pct = record.get("populationPercent").asFloat(0f);
    final var state = record.get("state").asString("");
    return IndexStatus.builder().progress(pct).state(toState(state)).build();
  }

  IndexStatus.State toState(String state) {
    if (state.equalsIgnoreCase("FAILED")) {
      return State.FAILED;
    }
    if (state.equalsIgnoreCase("ONLINE")) {
      return State.ONLINE;
    }
    if (state.equalsIgnoreCase("POPULATING")) {
      return State.POPULATING;
    }
    return State.OTHER;
  }

  public void writeIndexes(File file, List<IndexData> indexes) {
    final var objectMapper = new ObjectMapper();
    try (final var wrt = new BufferedWriter(new FileWriter(file))) {
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
          tx -> {
            final var result = tx.run("show indexes;");
            return result.list().stream()
                .map(IndexManager::fromRecord)
                .collect(Collectors.toList());
          });
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

  public Set<String> readIndexNames() {
    return readIndexes().stream().map(IndexData::getName).collect(toUnmodifiableSet());
  }

  public void createAndMonitor(IndexData index, boolean recreate) {
    if (recreate) {
      dropIndex(index);
    }
    createIndex(index);
    monitorCreation(index);
  }

  public long labelSize(String labelName) {
    final String FMT = "MATCH (n:`%s`) return count(n) as count";
    return this.readTransaction(
        tx -> {
          final Result result = tx.run(String.format(FMT, labelName));
          return Optional.ofNullable(Iterables.getFirst(result.list(), null))
              .map(r -> r.get(0).asLong(0L))
              .orElse(0L);
        });
  }

  /** Create all the indexes in one transaction for the bucket. */
  public void create(final Bucket bucket) {
    for (final IndexBatch batch : bucket.getBatches()) {
      // send all the commands
      try (final Session s = driver.session()) {
        final Transaction tx = s.beginTransaction();
        for (IndexData index : batch.getIndexes()) {
          final var q = createIndexQueryQuery(index);
          println(q);
          tx.run(q);
        }
        tx.commit();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      // monitor the indexes
      batch.getIndexes().forEach(this::monitorCreation);
    }
  }
}
