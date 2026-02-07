package org.neo4j.tool;

import lombok.RequiredArgsConstructor;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.tool.StoreCopyUtil.Flusher;
import org.neo4j.tool.dto.StoreCopyConfiguration;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.neo4j.tool.StoreCopyUtil.buildFlusher;
import static org.neo4j.tool.StoreCopyUtil.labelInSet;
import static org.neo4j.tool.StoreCopyUtil.toArray;

@RequiredArgsConstructor
public class StoreCopy {

    private final File source;
    private final File target;

    private final StoreCopyConfiguration configuration;

    private PrintWriter logs;

    public void run() throws Exception {
        final Pair<Long, Long> highestIds = getHighestNodeId(source);
        if (highestIds.first() < 0) {
            throw new IllegalArgumentException("Invalid source directory " + source);
        }
        final String pageCacheSize = System.getProperty("dbms.pagecache.memory", "2G");
        final BatchInserter sourceDb = BatchInserters.inserter(source, Collections.emptyMap());
        final Flusher flusher = buildFlusher(sourceDb);

        if (!target.mkdirs()) {
            throw new IllegalArgumentException("Unable to create directory for target copy - " + target.getAbsolutePath());
        }
        logs = new PrintWriter(new FileWriter(new File(target, "store-copy.log")));

        final Map<String, String> targetConfig = MapUtil.stringMap("dbms.pagecache.memory", pageCacheSize);
        BatchInserter targetDb = BatchInserters.inserter(target, targetConfig);
        RocksDBIdMap copiedNodeIds = copyNodes(sourceDb, targetDb, highestIds.first(), flusher);
        try {
            copyRelationships(sourceDb, targetDb, copiedNodeIds, highestIds.other(), flusher);
        } finally {
            copiedNodeIds.close();
        }
        System.out.println("Stopping target database");
        targetDb.shutdown();
        System.out.println("Stopped target database");
        try {
            System.out.println("Stopping source database");
            sourceDb.shutdown();
        }
        catch (Exception e) {
            logs.append(String.format("Noncritical error closing the source database:%n%s", Exceptions.stringify(e)));
        }
        System.out.println("Stopped source database");
        logs.close();
    }


    private static GraphDatabaseFactory factory() {
        try {
            final String className = "org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory";
            return (GraphDatabaseFactory) Class.forName(className).newInstance();
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            return new GraphDatabaseFactory();
        }
    }

    private static Pair<Long, Long> getHighestNodeId(File source) {
        GraphDatabaseAPI api = (GraphDatabaseAPI) factory().newEmbeddedDatabase(source);
        IdGeneratorFactory idGenerators = api.getDependencyResolver().provideDependency(IdGeneratorFactory.class).get();
        long highestNodeId = idGenerators.get(IdType.NODE).getHighestPossibleIdInUse();
        long highestRelId = idGenerators.get(IdType.RELATIONSHIP).getHighestPossibleIdInUse();
        api.shutdown();
        return Pair.of(highestNodeId, highestRelId);
    }

    private void copyRelationships(final BatchInserter sourceDb,
                                   final BatchInserter targetDb,
                                   final RocksDBIdMap copiedNodeIds,
                                   final long highestRelId,
                                   final Flusher flusher) {
        long time = System.currentTimeMillis();
        long relId = 0;
        long notFound = 0;
        long removed = 0;
        while (relId <= highestRelId) {
            BatchRelationship rel = null;
            String type = null;
            try {
                rel = sourceDb.getRelationshipById(relId++);
                type = rel.getType().name();
                if (!createRelationship(targetDb, sourceDb, rel, copiedNodeIds)) {
                    removed++;
                }
            }
            catch (Exception e) {
                if (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException && e.getMessage().endsWith("not in use")) {
                    notFound++;
                }
                else {
                    addLog(rel, "copy Relationship: " + (relId - 1) + "-[:" + type + "]" + "->?", e.getMessage());
                }
            }
            if (relId % 10000 == 0) {
                System.out.print(".");
                logs.flush();
            }
            if (relId % 500000 == 0) {
                flusher.flush();
                System.out.printf(" %d / %d (%d%%) unused %d removed %d%n", relId, highestRelId, percent(relId, highestRelId), notFound, removed);
            }
        }
        time = Math.max(1, (System.currentTimeMillis() - time) / 1000);
        System.out.printf("%n copying of %d relationship records took %d seconds (%d rec/s). Unused Records %d (%d%%) Removed Records %d (%d%%)%n",
                          relId, time, relId / time, notFound, percent(notFound, relId), removed, percent(removed, relId));
    }

    private static int percent(Number part, Number total) {
        return (int) (100 * part.floatValue() / total.floatValue());
    }

    private RocksDBIdMap copyNodes(BatchInserter sourceDb,
                                   BatchInserter targetDb,
                                   long highestNodeId,
                                   Flusher flusher) throws Exception {

        RocksDBIdMap copiedNodes = new RocksDBIdMap(target.getParentFile());
        long time = System.currentTimeMillis();
        long node = 0;
        long notFound = 0;
        long removed = 0;
        while (node <= highestNodeId) {
            try {
                if (sourceDb.nodeExists(node)) {
                    final Set<Label> nodeLabels = Iterables.asSet(sourceDb.getNodeLabels(node));
                    if (labelInSet(nodeLabels, configuration.getDeleteNodesWithLabels())) {
                        removed++;
                    }
                    else {
                        long newNodeId = targetDb.createNode(sourceDb.getNodeProperties(node), toArray(nodeLabels));
                        copiedNodes.put(node, newNodeId);
                    }
                }
                else {
                    notFound++;
                }
            }
            catch (Exception e) {
                if (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException && e.getMessage().endsWith("not in use")) {
                    notFound++;
                }
                else {
                    String msg = e.getMessage();
                    addLog(node, msg != null ? msg : e.getClass().getName());
                }
            }
            node++;
            if (node % 10000 == 0) {
                System.out.print(".");
            }
            if (node % 500000 == 0) {
                flusher.flush();
                logs.flush();
                System.out.printf(" %d / %d (%d%%) unused %d removed %d%n", node, highestNodeId, percent(node, highestNodeId), notFound, removed);
            }
        }
        time = Math.max(1, (System.currentTimeMillis() - time) / 1000);
        System.out.printf("%n copying of %d node records took %d seconds (%d rec/s). Unused Records %d (%d%%). Removed Records %d (%d%%).%n",
                          node, time, node / time, notFound, percent(notFound, node), removed, percent(removed, node));
        return copiedNodes;
    }

    private boolean createRelationship(BatchInserter targetDb,
                                       BatchInserter sourceDb,
                                       BatchRelationship rel,
                                       RocksDBIdMap copiedNodeIds) {

        long startNodeId = rel.getStartNode(), endNodeId = rel.getEndNode();
        if (copiedNodeIds != null) {
            startNodeId = copiedNodeIds.getOrDefault(startNodeId, -1L);
            endNodeId = copiedNodeIds.getOrDefault(endNodeId, -1L);
        }
        if (startNodeId == -1L || endNodeId == -1L) {
            return false;
        }
        final RelationshipType type = rel.getType();
        try {
            Map<String, Object> props = sourceDb.getRelationshipProperties(rel.getId());
            targetDb.createRelationship(startNodeId, endNodeId, type, props);
            return true;
        }
        catch (Exception e) {
            addLog(rel, "create Relationship: " + startNodeId + "-[:" + type + "]" + "->" + endNodeId, e.getMessage());
            return false;
        }
    }

    private void addLog(BatchRelationship rel, String property, String message) {
        logs.append(String.format("%s.%s %s%n", rel, property, message));
    }

    private void addLog(long node, String message) {
        logs.append(String.format("Node: %s %s%n", node, message));
    }

}
