package org.neo4j.tool;

import com.google.gson.GsonBuilder;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.tool.dto.IndexData;
import org.neo4j.tool.dto.IndexStatus;
import org.neo4j.tool.dto.IndexStatus.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.neo4j.tool.IndexUtils.buildIndex;
import static org.neo4j.tool.IndexUtils.indexOrConstraintQuery;
import static org.neo4j.tool.util.Print.println;

abstract class AbstractIndexCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractIndexCommand.class);

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
        try (val driver = buildDriver(uri, username, password, noAuth)) {
            execute(driver);
        }
        catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    abstract void execute(Driver driver) throws IOException;

    abstract File getFile();

    private Driver buildDriver(String uri, String username, String password, boolean noAuth) {
        // create the driver
        for (int i = 0; i < 5; i++) {
            try {
                val config = Config.defaultConfig();
                if (noAuth) {
                    println("Attempting to connect without authentication.");
                    return GraphDatabase.driver(uri, config);
                }
                println("Attempting to connect with basic authentication.");
                val token = AuthTokens.basic(username, password);
                return GraphDatabase.driver(uri, token, config);
            }
            catch (ServiceUnavailableException ex) {
                LOG.error("Failed to connect retrying..");
            }
        }
        throw new IllegalStateException("Unable to connect to Neo4J: " + uri);
    }

    //description	tokenNames	properties	state	type	progress	provider	failureMessage
    IndexData fromRecord(Record record) {
        return IndexData.builder()
            .id(record.get("id").asLong())
            .state(record.get("state").asString())
            .populationPercent(record.get("progress").asFloat())
            .uniqueness(record.get("type").asString("").contains("unique"))
            .type(record.get(5).asString())
            .labelsOrTypes(toList(record.get("tokenNames")))
            .properties(toList(record.get("properties")))
            .indexProvider(toIndexProvider(record.get("provider")))
            .build();
    }

    private static String toIndexProvider(Value provider) {
        return Optional.ofNullable(provider.asMap()).map(m -> (String) m.get("key")).orElse(null);
    }

    static List<String> toList(Value value) {
        return (null == value || value.isNull()) ? Collections.emptyList() : value.asList(Value::asString);
    }

    static void progressPercentage(int remain) {
        if (remain > 100) {
            throw new IllegalArgumentException();
        }
        int maxBareSize = 100; // 100 unit for 100%
        char defaultChar = '-';
        String icon = "*";

        String bare = new String(new char[maxBareSize]).replace('\0', defaultChar) + "]";
        StringBuilder bareDone = new StringBuilder("[");
        for (int i = 0; i < Math.max(0, remain); i++) {
            bareDone.append(icon);
        }
        String bareRemain = bare.substring(remain);
        System.out.print("\r" + bareDone + bareRemain + " " + remain + "%");
        if (remain == 100) {
            System.out.print("\n");
        }
    }

    List<IndexData> readIndexesFromFilename() {
        return readIndexesFromFile(getFile());
    }

    List<IndexData> readIndexesFromFile(File f) {
        val ret = new ArrayList<IndexData>();
        val gson = new GsonBuilder().create();
        try (val rdr = new BufferedReader(new FileReader(f))) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                val index = gson.fromJson(line, IndexData.class);
                ret.add(index);
            }
        }
        catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
        return ret;
    }

    void createIndexOrConstraint(final Driver driver, IndexData index) {
        val query = indexOrConstraintQuery(index);
        println(query);
        try {
            writeTransaction(driver, query);
        }
        catch (Throwable th) {
            LOG.error("Failed to create index: {}", query, th);
            throw new RuntimeException(th);
        }
    }

    boolean validIndex(final Driver driver, final IndexData index) {
        // insure index creation started
        for (int i = 0; i < 10; i++) {
            val status = indexProgress(driver, index);
            if (null != status && status.getState().isOk()) {
                return true;
            }
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    void createIndexWaitForCompletion(final Driver driver, final IndexData index) {
        createIndexOrConstraint(driver, index);
        val q = indexOrConstraintQuery(index);
        if (!validIndex(driver, index)) {
            println("Failed to execute: %s", q);
            return;
        }

        // wait for completion
        int pct = 0;
        while (pct < 100) {
            progressPercentage(pct);
            val status = indexProgress(driver, index);
            if (status.getState().isFailed()) {
                println("%nFailed to execute: %s", q);
                return;
            }
            pct = (int) status.getProgress();
            progressPercentage(pct);
        }
    }

    void writeTransaction(final Driver driver, final String query) {
        // query for all the indexes
        try (Session session = driver.session()) {
            assert session != null;
            val resultSummary = session.writeTransaction(tx -> tx.run(query).consume());
            if (LOG.isDebugEnabled()) {
                LOG.debug(resultSummary.toString());
            }
        }
    }

    String propertiesArgument(IndexData indexData) {
        final UnaryOperator<String> propFx = p -> '`' + p + '`';
        return indexData.getProperties().stream().map(propFx).collect(joining(","));
    }

    IndexStatus indexProgress(final Driver driver, IndexData index) {
        // query for all the indexes
        try (final Session session = driver.session()) {
            assert session != null;
            return session.readTransaction(tx -> toIndexState(tx, index));
        }
    }

    IndexStatus toIndexState(Transaction tx, IndexData index) {
        val fmt = "call db.indexes() yield description, state, progress " +
                "WHERE description contains \"%s\" return state, progress";
        val indexName = buildIndex(index);
        val result = tx.run(String.format(fmt, indexName));
        val record = Iterables.firstOrNull(result.list());
        if (null == record) {
            return IndexStatus.builder().state(State.FAILED).build();
        }
        val pct = record.get("progress").asFloat(0);
        val state = record.get("state").asString("");
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

    void writeIndexes(List<IndexData> indexes) {
        val gson = new GsonBuilder().create();
        try (val wrt = new BufferedWriter(new FileWriter(getFile()))) {
            for (IndexData index : indexes) {
                wrt.write(gson.toJson(index));
                wrt.newLine();
            }
        }
        catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * Read all the index and constraints in order, of constrains first.
     */
    List<IndexData> readIndexes(Driver driver) {
        try (val session = driver.session()) {
            assert session != null;
            return session.readTransaction(
                tx -> tx.run("call db.indexes();")
                    .list()
                    .stream()
                    .map(this::fromRecord)
                    .collect(Collectors.toList()));
        }
    }

    String dropQuery(final IndexData data) {
        val label = Iterables.firstOrNull(data.getLabelsOrTypes());
        if (data.isUniqueness()) {
            val fmt = "DROP CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE;";
            val firstProp = Iterables.firstOrNull(data.getProperties());
            return String.format(fmt, label, firstProp);
        }
        val fmt = "DROP INDEX ON :`%s`(%s);";
        return String.format(fmt, label, propertiesArgument(data));
    }

    void dropIndex(final Driver driver, final IndexData indexData) {
        // query for all the indexes
        val query = dropQuery(indexData);
        println(query);
        try {
            writeTransaction(driver, query);
        }
        catch (Throwable th) {
            LOG.error("Failed to drop index: {}", query, th);
        }
    }
}
