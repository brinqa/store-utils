package com.brinqa.neo4j.tool;

import com.brinqa.neo4j.tool.dto.IndexData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

/** Attempt to recreate all the indexes */
public class DumpLoadIndexUnitTest {
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
    public void indexLoad() throws Exception {
        final var mapper = new ObjectMapper();
        final var f = new File("dump-index.json");
        final var typedef = new TypeReference<List<IndexData>>() {};
        final var indexes = mapper.readValue(f, typedef);

        // indexes.stream().map(IndexData::getType).distinct().forEach(System.out::println);
        // System.out.println(indexes);
        final Map<String, IndexData> nameToIndex = new HashMap<>();
        for (IndexData index : indexes) {
            nameToIndex.put(index.getName(), index);
        }

        indexes.forEach(
                index -> {
                    // owning constraint exists it's unique
                    //            if (null != index.getOwningConstraint()) {
                    //                System.out.println(index);
                    //                var owner = nameToIndex.get(index.getOwningConstraint());
                    //                System.out.println(owner);
                    //            }

                    // index provider
                    if (null != index.getIndexProvider()) {
                        System.out.println(index);
                    }
                });
    }
}
