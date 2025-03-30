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
package com.brinqa.neo4j.tool;

import static com.brinqa.neo4j.tool.util.Print.println;

import java.io.IOException;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import com.brinqa.neo4j.tool.index.IndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

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
        try (final var driver = buildDriver(uri, username, password, noAuth)) {
            execute(new IndexManager(driver));
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    abstract void execute(IndexManager manager) throws IOException;

    private Driver buildDriver(String uri, String username, String password, boolean noAuth) {
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
                LOG.error("Failed to connect retrying..");
            }
        }
        throw new IllegalStateException("Unable to connect to Neo4J: " + uri);
    }
}
