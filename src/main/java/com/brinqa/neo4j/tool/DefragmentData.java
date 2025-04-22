package com.brinqa.neo4j.tool;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilderImplementation;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import picocli.CommandLine;
import reactor.core.publisher.Flux;

/** CLI to defragment data in Neo4j database, copying from a source directory to a target one. */
@CommandLine.Command(
    name = "defragment",
    version = "defragment 1.0",
    description =
        "Defragments data in Neo4j database, copying from a source directory to a target one.")
public class DefragmentData extends AbstractAdminCommand {

  /** Target directory to copy the data to. */
  @CommandLine.Option(
      names = {"-t", "--target"},
      description = "Target directory to copy the data to.")
  File targetDirectory;

  @CommandLine.Option(names = "--from-path", description = "Path to databases directory.")
  private Path path;

  // use the default
  private final StorageEngineFactory.Selector storageEngineSelector = StorageEngineFactory.SELECTOR;

  /** Initialize the environment based on the given context. */
  public DefragmentData(ExecutionContext ctx) {
    super(ctx);
  }

  /** Execute the command to defragment the data. */
  @Override
  protected void execute() throws Exception {
    final var ourConfig = this.createConfig();
    final var neo4jLayout = Neo4jLayout.of(ourConfig);
    try (var fs = this.ctx.fs()) {
      this.validateDatabasesPath(fs, neo4jLayout.databasesDirectory());

      // build management service
      var managementService = buildDatabaseManagementService(neo4jLayout, ourConfig);
      // find the default database
      var database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
      managementService.createDatabase("defragment_db");
      var targetDb = (GraphDatabaseAPI) managementService.database("defragment_db");

      // copy nodes
      int totalNodes = copyNodes(database, targetDb);
      System.out.println("Total nodes: " + totalNodes);

      // copy relationships
      int totalRelationships = copyRelationships(database, targetDb);
      System.out.println("Total relationships: " + totalRelationships);

    } catch (Exception e) {
      throw new CommandFailedException(
          String.format("Failed to execute command: '%s'.", e.getMessage()), e);
    }
  }

  /** without a batch importer this feels like will be slow. */
  int copyNodes(GraphDatabaseAPI source, GraphDatabaseAPI target) {
    var ret =
        Flux.using(source::beginTx, tx -> Flux.fromIterable(tx.getAllNodes()), Transaction::close)
            .buffer()
            .flatMap(
                buffer -> {
                  // create the batch of nodes
                  try (Transaction targetTx = target.beginTx()) {
                    for (final Node node : buffer) {
                      final Label[] labels = Iterables.asArray(Label.class, node.getLabels());
                      var newNode = targetTx.createNode(labels);
                      for (var property : node.getAllProperties().entrySet()) {
                        newNode.setProperty(property.getKey(), property.getValue());
                      }
                    }
                    targetTx.commit();
                    return Flux.just(buffer.size());
                  }
                })
            .reduce(Integer::sum)
            .block();
    return Optional.ofNullable(ret).orElse(0);
  }

  /** without a batch importer this feels like will be slow. */
  int copyRelationships(GraphDatabaseAPI source, GraphDatabaseAPI target) {
    try (Transaction tx = source.beginTx()) {
      Integer ret =
          Flux.fromIterable(tx.getAllRelationships())
              .buffer()
              .map(
                  buffer -> {
                    // create the batch of nodes
                    try (var targetTx = target.beginTx()) {
                      for (var relationship : buffer) {
                        createRelationship(targetTx, relationship);
                      }
                      targetTx.commit();
                      return buffer.size();
                    }
                  })
              .reduce(Integer::sum)
              .block();
      return Optional.ofNullable(ret).orElse(0);
    }
  }

  void createRelationship(Transaction tx, Relationship relationship) {
    var targetStartNode = tx.getNodeByElementId(relationship.getStartNode().getElementId());
    var targetEndNode = tx.getNodeByElementId(relationship.getEndNode().getElementId());
    var type = relationship.getType();
    // create the relationship
    var targetRelationship = targetStartNode.createRelationshipTo(targetEndNode, type);
    // set all the properties
    relationship.getAllProperties().forEach(targetRelationship::setProperty);
  }

  private DatabaseManagementService buildDatabaseManagementService(
      Neo4jLayout neo4jLayout, Config ourConfig) {
    var databaseManagementServiceBld =
        new DatabaseManagementServiceBuilderImplementation(neo4jLayout.homeDirectory()) {
          @Override
          protected DatabaseManagementService newDatabaseManagementService(
              Config na, ExternalDependencies dependencies) {
            return super.newDatabaseManagementService(ourConfig, dependencies);
          }
        };
    return databaseManagementServiceBld.build();
  }

  private Config createConfig() {
    final var builder =
        createPrefilledConfigBuilder().set(GraphDatabaseSettings.read_only_database_default, true);
    if (path != null) {
      builder.set(
          GraphDatabaseInternalSettings.databases_root_path, path.toAbsolutePath().normalize());
    }
    return builder.build();
  }

  private void validateDatabasesPath(FileSystemAbstraction fs, Path databasesPath) {
    if (!fs.isDirectory(databasesPath)) {
      throw new IllegalArgumentException(
          format("Provided path %s must point to a directory.", databasesPath));
    }

    // check not a path to store files
    final var databaseLayout = DatabaseLayout.ofFlat(databasesPath);
    if (Validators.isExistingDatabase(storageEngineSelector, fs, databaseLayout)) {
      throw new IllegalArgumentException(
          format(
              "The directory %s contains the store files of a single database."
                  + " --from-path should point to the databases directory.",
              databasesPath));
    }
  }
}
