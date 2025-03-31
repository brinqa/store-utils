package com.brinqa.neo4j.tool;

import static com.brinqa.neo4j.tool.util.Print.println;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

@Slf4j
@UtilityClass
public class Neo4jHelper {

    public Driver buildDriver(String uri, String username, String password, boolean noAuth) {
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
