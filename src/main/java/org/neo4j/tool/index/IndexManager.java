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
package org.neo4j.tool.index;

import com.google.common.collect.Iterables;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;
import org.neo4j.tool.dto.Bucket;
import org.neo4j.tool.dto.ConstraintStatus;
import org.neo4j.tool.dto.IndexBatch;
import org.neo4j.tool.dto.IndexData;
import org.neo4j.tool.dto.IndexStatus;
import org.neo4j.tool.dto.IndexStatus.State;

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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.neo4j.tool.util.Print.println;
import static org.neo4j.tool.util.Print.progressPercentage;

@Slf4j
@AllArgsConstructor
public class IndexManager {

    private final Driver driver;

    static IndexData fromRecord(Record record) {
        return IndexData.builder()
                .id(record.get(0).asLong())
                .name(record.get(1).asString())
                .state(record.get(2).asString())
                .populationPercent(record.get(3).asFloat())
                .uniqueness("UNIQUE".equalsIgnoreCase(record.get(4).asString()))
                .type(record.get(5).asString())
                .entityType(record.get(6).asString())
                .labelsOrTypes(toList(record.get(7)))
                .properties(toList(record.get(8)))
                .indexProvider(record.get(9).asString())
                .build();
    }

    static List<String> toList(Value value) {
        return (null == value || value.isNull()) ? List.of() : value.asList(Value::asString);
    }

    public List<IndexData> readIndexesFromFile(File f) {
        final var ret = new ArrayList<IndexData>();
        final var gson = new GsonBuilder().create();
        try (final var rdr = new BufferedReader(new FileReader(f))) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                final var index = gson.fromJson(line, IndexData.class);
                ret.add(index);
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
        return ret;
    }

    void createIndex(IndexData index) {
        final var query = indexOrConstraintQuery(index);
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
            final var status = indexProgress(index.getName());
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
        while (pct < 100) {
            progressPercentage(pct);
            final var status = indexProgress(index.getName());
            if (status.getState().isFailed()) {
                println("%nFailed to create index: %s", index.getName());
                return;
            }
            pct = (int) status.getProgress();
            progressPercentage(pct);
            simpleWait(100);
        }

        // index creation is finished
        if (!index.isUniqueness()) {
            return;
        }

        // loop waiting for a bit for it to be created fail after 10 secs
        for (int i = 0; i < 100; i++) {
            final var status = constraintCheck(index.getName());
            if (status.isOnline()) {
                return;
            }
            simpleWait(100);
        }
        final var ERROR_FMT = "Constraint '%s' failed to come online, please create manually.";
        throw new IllegalStateException(String.format(ERROR_FMT, index.getName()));
    }

    void writeTransaction(final String query) {
        // query for all the indexes
        try (final Session session = driver.session()) {
            assert session != null;
            var resultSummary = session.readTransaction(tx -> tx.run(query).consume());
            if (log.isDebugEnabled()) {
                log.debug(resultSummary.toString());
            }
        }
    }

    <T> T readTransaction(TransactionWork<T> work) {
        // query for all the indexes
        final var cfg = SessionConfig.builder().withDefaultAccessMode(AccessMode.READ).build();
        try (final Session session = driver.session(cfg)) {
            assert session != null;
            return session.readTransaction(work);
        }
    }

    String createConstraintFormat() {
        return "CREATE CONSTRAINT `%s` IF NOT EXISTS FOR (n:`%s`) REQUIRE n.%s IS UNIQUE;";
    }

    String indexQuery(IndexData indexData) {
        final String name = indexData.getName();
        final String label = Iterables.getFirst(indexData.getLabelsOrTypes(), null);

        // create an index
        final String IDX_FMT =
                "CREATE INDEX %s IF NOT EXISTS FOR (n:`%s`) ON (%s) OPTIONS { indexProvider: '%s' };";
        final String indexProvider = indexData.getIndexProvider();
        // make sure to quote all the properties of an index
        final UnaryOperator<String> propFx = p -> "n.`" + p + "`";
        final String properties =
                indexData.getProperties().stream().map(propFx).collect(joining(","));
        return String.format(IDX_FMT, name, label, properties, indexProvider);
    }

    String constraintQuery(IndexData indexData) {
        final String name = indexData.getName();
        final String label = Iterables.getFirst(indexData.getLabelsOrTypes(), null);

        // create constraint
        final String format = createConstraintFormat();
        final String firstProp = Iterables.getFirst(indexData.getProperties(), null);
        return String.format(format, name, label, firstProp);
    }

    public String indexOrConstraintQuery(IndexData indexData) {
        return indexData.isUniqueness()
                ? constraintQuery(indexData)
                : indexQuery(indexData);
    }

    IndexStatus indexProgress(String name) {
        // query for all the indexes
        try (final Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(tx -> toIndexState(tx, name));
        }
    }

    ConstraintStatus constraintCheck(String name) {
        // query for all the indexes
        try (final Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(tx -> toConstraintStatus(tx, name));
        }
    }

    ConstraintStatus toConstraintStatus(Transaction tx, String name) {
        final String FMT = "show constraints yield name WHERE name = \"%s\"";
        final var result = tx.run(String.format(FMT, name));
        final var record = Iterables.getFirst(result.list(), null);
        return ConstraintStatus.of(null != record);
    }

    IndexStatus toIndexState(Transaction tx, String name) {
        final String FMT = "show indexes yield populationPercent,state,name WHERE name = \"%s\"";
        final var result = tx.run(String.format(FMT, name));
        final var record = result.single();
        if (null == record) {
            return IndexStatus.builder().state(State.FAILED).build();
        }
        final var pct = record.get("populationPercent").asFloat();
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

    public void writeIndexes(List<IndexData> indexes, File file) {
        final var gson = new GsonBuilder().create();
        try (final var wrt = new BufferedWriter(new FileWriter(file))) {
            for (IndexData index : indexes) {
                wrt.write(gson.toJson(index));
                wrt.newLine();
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * Read all the index and constraints in order, of constrains first.
     */
    public List<IndexData> readDBIndexes() {
        try (Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(
                    tx -> {
                        final var result = tx.run("show indexes;");
                        return result.list().stream()
                                .map(IndexManager::fromRecord)
                                .collect(Collectors.toList());
                    });
        }
    }

    String dropQuery(final IndexData data) {
        final var FMT =
                (data.isUniqueness() ? "DROP CONSTRAINT %s" : "DROP INDEX %s") + " IF EXISTS;";
        return String.format(FMT, data.getName());
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
        return readDBIndexes().stream().map(IndexData::getName).collect(toUnmodifiableSet());
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
                    final var query = String.format(FMT, labelName);
                    final var result = tx.run(query);
                    return Optional.ofNullable(Iterables.getFirst(result.list(), null))
                            .map(r -> r.get(0).asLong(0L))
                            .orElse(0L);
                });
    }

    /**
     * Create all the indexes in one transaction for the bucket.
     */
    public void create(final Bucket bucket) {
        for (final IndexBatch batch : bucket.getBatches()) {
            // send all the commands
            try (final Session s = driver.session()) {
                final Transaction tx = s.beginTransaction();
                for (IndexData index : batch.getIndexes()) {
                    final var q = indexOrConstraintQuery(index);
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
