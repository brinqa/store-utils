package com.brinqa.neo4j.tool;

import com.brinqa.neo4j.tool.dto.IndexData;
import com.brinqa.neo4j.tool.index.IndexManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

public class DumpIndexTest {
    private static final Neo4jContainer<?> neo4j =
            new Neo4jContainer<>(DockerImageName.parse("neo4j:latest"));

    // create the driver
    private static Driver driver;

    @BeforeClass
    public static void init() {
        neo4j.start();
        // create some indexes
        final var uri = neo4j.getBoltUrl();
        final var username = "neo4j";
        final var password = neo4j.getAdminPassword();

        driver = Neo4jHelper.buildDriver(uri, username, password, false);
    }

    @AfterClass
    public static void close() {
        driver.close();
        // neo4j.close();
    }

    @Test
    public void dumpIndex() throws Exception {
        final var indexMgr = new IndexManager(driver);

        // create a simple index
        driver.session()
                .executeWrite(
                        context -> {
                            context.run(
                                    "CREATE INDEX testone_prop IF NOT EXISTS FOR (n:`OneProperty`) ON (n.ipAddress);");
                            return null;
                        });

        // create a 2 property index
        driver.session()
                .executeWrite(
                        context -> {
                            context.run(
                                    "CREATE INDEX testtwo_prop IF NOT EXISTS FOR (n:`TwoProperty`) ON (n.ipAddress, n.name);");
                            return null;
                        });

        driver.session()
                .executeWrite(
                        context -> {
                            context.run(
                                    "CREATE (nilsE:Employee {name: \"Nils-Erik Karlsson\", position: \"Engineer\", team: \"Kernel\", peerReviews: ['Nils-Erik is difficult to work with.', 'Nils-Erik is often late for work.']}),\n"
                                            + "(lisa:Manager {name: \"Lisa Danielsson\", position: \"Engineering manager\"}),\n"
                                            + "(nils:Employee {name: \"Nils Johansson\", position: \"Engineer\", team: \"Operations\"}),\n"
                                            + "(maya:Employee {name: \"Maya Tanaka\", position: \"Senior Engineer\", team:\"Operations\"}),\n"
                                            + "(lisa)-[:REVIEWED {message: \"Nils-Erik is reportedly difficult to work with.\"}]->(nilsE),\n"
                                            + "(maya)-[:EMAILED {message: \"I have booked a team meeting tomorrow.\"}]->(nils)");
                            return null;
                        });

        driver.session()
                .executeWrite(
                        context -> {
                            context.run(
                                    "CREATE FULLTEXT INDEX namesAndTeams FOR (n:Employee|Manager) ON EACH [n.name, n.team]");
                            return null;
                        });

        // should be basically empty
        final var mapper = new ObjectMapper();
        final var indexes = indexMgr.readIndexes();
        for (IndexData index : indexes) {
            System.out.println(mapper.writeValueAsString(index));
        }
        File f = new File("dump-index.json");
        mapper.writeValue(f, indexes);
    }

    @Test
    public void indexLoad() throws Exception {
        final var mapper = new ObjectMapper();
        final var f = new File("dump-index.json");
        final var typedef = new TypeReference<List<IndexData>>() {};
        final var indexes = mapper.readValue(f, typedef);

        final var mgr = new IndexManager(driver);
        for (IndexData index : indexes) {
            mgr.createAndMonitor(index, true);
        }
    }
}
