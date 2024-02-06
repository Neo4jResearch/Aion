/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.temporalprocs.benchmarks;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.printResults;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.harness.Neo4j;
import org.neo4j.temporalgraph.benchmarks.TemporalStore;
import org.neo4j.temporalgraph.benchmarks.data.GraphLoader;
import org.neo4j.temporalprocs.result.ResultStore;

public class BenchmarkUtils {

    private static final Label LABEL = Label.label("Node");

    private enum RelTypes implements RelationshipType {
        IS_CONNECTED_TO
    }

    private static final String NODE_PROPERTY_NAME = "nodeId";
    private static final String START_TIME = "startTime";
    private static final String END_TIME = "endTime";
    private static final String TIMESTAMP = "timestamp";
    private static final String GRAPH_NAME = "graph";

    private static final boolean RUN_NON_INCR = true;
    private static final int ITERATIONS = 3;
    private static final int NUMBER_OF_WORKERS = 1; // WorkerThreadPool.THREAD_NUMBER;

    private BenchmarkUtils() {}

    private static int runCallables(List<Callable<Integer>> callableTasks) {
        var threadPool = WorkerThreadPool.getInstance();
        try {
            var counter = 0;
            var futures = threadPool.invokeAll(callableTasks);
            for (var f : futures) {
                counter += f.get();
            }
            return counter;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static void importGraphToDB(GraphLoader graphLoader, Neo4j db, TemporalStore store) {
        final var graphDb = db.databaseManagementService().database(DEFAULT_DATABASE_NAME);
        final int batchSize = 10000;

        // Create an index to accelerate node lookups
        try (Transaction tx = graphDb.beginTx()) {
            Schema schema = tx.schema();
            schema.constraintFor(LABEL)
                    .assertPropertyIsUnique(NODE_PROPERTY_NAME)
                    .withName(NODE_PROPERTY_NAME)
                    .create();
            tx.commit();
        }
        // and wait for its initialization
        try (Transaction tx = graphDb.beginTx()) {
            Schema schema = tx.schema();
            schema.awaitIndexesOnline(10, TimeUnit.SECONDS);
        }

        List<Callable<Integer>> callableTasks = new ArrayList<>();
        var nodeSet = graphLoader.getNodes();
        final int numberOfWorkers = WorkerThreadPool.THREAD_NUMBER;
        for (int i = 0; i < numberOfWorkers; ++i) {
            long finalI = i;
            callableTasks.add(() -> {
                var counter = 0;
                var nodeIterator = nodeSet.iterator();
                while (nodeIterator.hasNext()) {
                    try (Transaction tx = graphDb.beginTx()) {
                        int j = 0;
                        while (j < batchSize && nodeIterator.hasNext()) {
                            var id = nodeIterator.next();
                            if (id % numberOfWorkers == finalI) {
                                var node = tx.createNode(LABEL);
                                node.setProperty(NODE_PROPERTY_NAME, id);
                                j++;
                                counter++;
                            }
                        }
                        tx.commit();
                    }
                }
                return counter;
            });
        }
        runCallables(callableTasks);

        callableTasks = new ArrayList<>();
        var relationships = graphLoader.getRelationships();
        for (int i = 0; i < numberOfWorkers; ++i) {
            long finalI = i;
            callableTasks.add(() -> {
                var counter = 0;
                var relIterator = relationships.iterator();
                while (relIterator.hasNext()) {
                    try (Transaction tx = graphDb.beginTx()) {
                        int j = 0;
                        while (j < batchSize && relIterator.hasNext()) {
                            var rel = relIterator.next();
                            if (rel.getEntityId() % numberOfWorkers == finalI) {
                                var startNode = tx.findNode(LABEL, NODE_PROPERTY_NAME, rel.getStartNode());
                                var endNode = tx.findNode(LABEL, NODE_PROPERTY_NAME, rel.getEndNode());
                                var relationship = startNode.createRelationshipTo(endNode, RelTypes.IS_CONNECTED_TO);
                                relationship.setProperty("relId", rel.getEntityId());
                                j++;
                                counter++;
                            }
                        }
                        tx.commit();
                    }
                }
                return counter;
            });
        }
        runCallables(callableTasks);

        // Call checkpoint to flush all data to disk
        try (var session = getDriver().session()) {
            session.run("CALL db.checkpoint();").consume();
        }
        if (store != null) {
            store.flush();
        }
    }

    public static int runFetchEntities(int numberOfRelationships, int numberOfOperations) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        final int numberOfWorkers = WorkerThreadPool.THREAD_NUMBER;
        for (int i = 0; i < numberOfWorkers; ++i) {
            int finalNumberOfOperations = numberOfOperations / numberOfWorkers;
            long finalI = i;
            callableTasks.add(() -> {
                var counter = 0;
                Random random = new Random(finalI);
                try (var session = getDriver().session()) {
                    for (int op = 0; op < finalNumberOfOperations; ++op) {
                        var relId = org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.randInt(
                                random, 0, numberOfRelationships - 1);
                        var result = session.run(
                                "CALL lineage.getTemporalRelationship($relId, $timestamp);",
                                parameters("relId", relId, TIMESTAMP, relId));
                        if (result.hasNext()) {
                            counter++;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runFetchAndWriteEntities(int numberOfNodes, int numberOfRelationships, int numberOfOperations) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        final int numberOfWorkers = WorkerThreadPool.THREAD_NUMBER;
        for (int i = 0; i < numberOfWorkers; ++i) {
            int finalNumberOfOperations = numberOfOperations / numberOfWorkers;
            long finalI = i;
            callableTasks.add(() -> {
                var counter = 0;
                Random random = new Random(finalI);
                try (var session = getDriver().session()) {
                    for (int op = 0; op < finalNumberOfOperations; ++op) {
                        if (op % 100 < 10) {
                            var nodeId1 = org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.randInt(
                                    random, numberOfNodes, 2 * numberOfNodes - 1);
                            var nodeId2 = org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.randInt(
                                    random, numberOfNodes, 2 * numberOfNodes - 1);
                            var relId = org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.randInt(
                                    random, numberOfRelationships, 2 * numberOfRelationships - 1);
                            var result = session.run(
                                    "CREATE (n1:Node {nodeId: $nodeId1}), (n2:Node {nodeId: $nodeId2}),"
                                            + "(n1)-[r:IS_CONNECTED_TO {relId: $relId}]->(n2) "
                                            + "RETURN r;",
                                    parameters("nodeId1", nodeId1, "nodeId2", nodeId2, "relId", relId));
                            if (result.hasNext()) {
                                counter++;
                            }
                        } else {
                            var relId = org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.randInt(
                                    random, 0, numberOfRelationships - 1);
                            var result = session.run(
                                    "CALL lineage.getTemporalRelationship($relId, $timestamp);",
                                    parameters("relId", relId, TIMESTAMP, relId));
                            if (result.hasNext()) {
                                counter++;
                            }
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runFetchAllRelationships(TemporalStore store, int numberOfOperations) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        final int numberOfWorkers = WorkerThreadPool.THREAD_NUMBER;
        for (int i = 0; i < numberOfWorkers; ++i) {
            long finalI = i;
            int finalNumberOfOperations = numberOfOperations / numberOfWorkers;
            callableTasks.add(() -> {
                var counter = 0;
                Random random = new Random(finalI);
                try (var session = getDriver().session()) {
                    for (int op = 0; op < finalNumberOfOperations; ++op) {
                        var timestamp = org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.randInt(
                                random, 0, (int) store.getLastTimestamp());
                        var result = session.run(
                                "CALL time.getTemporalAllRelationships($timestamp);", parameters(TIMESTAMP, timestamp));
                        if (result.hasNext()) {
                            counter++;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runBFSCallables(
            TemporalStore store, int numberOfOperations, int numberOfSnapshots, int maxSnapshots, int maxNodeId) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_WORKERS; ++i) {
            long finalI = i;
            callableTasks.add(() -> {
                var counter = 0;
                Random random = new Random(finalI);
                try (var session = getDriver().session()) {
                    for (var o = 0; o < numberOfOperations; ++o) {
                        var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                        var timestamp = store.getLastTimestamp() / 2;
                        var sourceId =
                                org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.randInt(random, 0, maxNodeId - 1);
                        for (int s = 0; s < numberOfSnapshots; s++) {
                            var result = session.run(
                                    "CALL time.dynamicBfsMutate($nodeId, $startTime, $endTime, $step);",
                                    parameters(
                                            NODE_PROPERTY_NAME,
                                            sourceId,
                                            START_TIME,
                                            timestamp,
                                            END_TIME,
                                            timestamp,
                                            "step",
                                            timestamp));
                            if (result.hasNext()) {
                                counter++;
                            }

                            timestamp += step;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runBFSIncrementalCallables(
            TemporalStore store, int numberOfOperations, int numberOfSnapshots, int maxSnapshots, int maxNodeId) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_WORKERS; ++i) {
            long finalI = i;
            callableTasks.add(() -> {
                var counter = 0;
                Random random = new Random(finalI);
                try (var session = getDriver().session()) {
                    for (var o = 0; o < numberOfOperations; ++o) {
                        var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                        var timestamp = store.getLastTimestamp() / 2;
                        var maxTimestamp = timestamp + step * (numberOfSnapshots - 1);
                        var sourceId =
                                org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.randInt(random, 0, maxNodeId - 1);
                        var result = session.run(
                                "CALL time.dynamicBfsMutate($nodeId, $startTime, $endTime, $step);",
                                parameters(
                                        NODE_PROPERTY_NAME,
                                        sourceId,
                                        START_TIME,
                                        timestamp,
                                        END_TIME,
                                        maxTimestamp,
                                        "step",
                                        step));
                        if (result.hasNext()) {
                            counter++;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runBFS(TemporalStore store, String datasetName, int maxNodeId) {
        var counter = 0;
        var snapshots = new int[] {10, 100};
        var operations = new int[] {ITERATIONS, ITERATIONS};
        var maxSnapshots = Arrays.stream(snapshots).max().getAsInt();

        ResultStore.getInstance().clear();
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var totalOps = ops * NUMBER_OF_WORKERS;
            var startWorkload = System.nanoTime();

            counter += (RUN_NON_INCR) ? runBFSCallables(store, ops, numberOfSnapshots, maxSnapshots, maxNodeId) : 0;

            var endWorkload = System.nanoTime();
            printResults(datasetName, "BFS", totalOps, numberOfSnapshots, startWorkload, endWorkload);
        }

        ResultStore.getInstance().clear();
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var totalOps = ops * NUMBER_OF_WORKERS;
            var startWorkload = System.nanoTime();

            counter += runBFSIncrementalCallables(store, ops, numberOfSnapshots, maxSnapshots, maxNodeId);

            var endWorkload = System.nanoTime();
            printResults(datasetName, "BFSIncr", totalOps, numberOfSnapshots, startWorkload, endWorkload);
        }
        return counter;
    }

    public static int runPRCallables(
            TemporalStore store, int numberOfOperations, int numberOfSnapshots, int maxSnapshots) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_WORKERS; ++i) {
            callableTasks.add(() -> {
                var counter = 0;
                try (var session = getDriver().session()) {
                    for (var o = 0; o < numberOfOperations; ++o) {
                        var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                        var timestamp = store.getLastTimestamp() / 2;
                        for (int s = 0; s < numberOfSnapshots; s++) {
                            var result = session.run(
                                    "CALL time.dynamicPageRankMutate($startTime, $endTime, $step);",
                                    parameters(START_TIME, timestamp, END_TIME, timestamp, "step", timestamp));
                            if (result.hasNext()) {
                                counter++;
                            }

                            timestamp += step;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runPRIncrementalCallables(
            TemporalStore store, int numberOfOperations, int numberOfSnapshots, int maxSnapshots) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_WORKERS; ++i) {
            callableTasks.add(() -> {
                var counter = 0;
                try (var session = getDriver().session()) {
                    for (var o = 0; o < numberOfOperations; ++o) {
                        var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                        var timestamp = store.getLastTimestamp() / 2;
                        var maxTimestamp = timestamp + step * (numberOfSnapshots - 1);
                        var result = session.run(
                                "CALL time.dynamicPageRankMutate($startTime, $endTime, $step);",
                                parameters(START_TIME, timestamp, END_TIME, maxTimestamp, "step", step));
                        if (result.hasNext()) {
                            counter++;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runPageRank(TemporalStore store, String datasetName) {
        var counter = 0;
        var snapshots = new int[] {10, 100};
        var operations = new int[] {ITERATIONS, ITERATIONS};
        var maxSnapshots = Arrays.stream(snapshots).max().getAsInt();

        ResultStore.getInstance().clear();
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var totalOps = ops * NUMBER_OF_WORKERS;
            var startWorkload = System.nanoTime();

            counter += (RUN_NON_INCR) ? runPRCallables(store, ops, numberOfSnapshots, maxSnapshots) : 0;

            var endWorkload = System.nanoTime();
            printResults(datasetName, "PR", totalOps, numberOfSnapshots, startWorkload, endWorkload);
        }

        ResultStore.getInstance().clear();
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var totalOps = ops * NUMBER_OF_WORKERS;
            var startWorkload = System.nanoTime();

            counter += runPRIncrementalCallables(store, ops, numberOfSnapshots, maxSnapshots);

            var endWorkload = System.nanoTime();
            printResults(datasetName, "PRIncr", totalOps, numberOfSnapshots, startWorkload, endWorkload);
        }
        return counter;
    }

    public static int runNSCallables(
            TemporalStore store, int numberOfOperations, int numberOfSnapshots, int maxSnapshots) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_WORKERS; ++i) {
            callableTasks.add(() -> {
                var counter = 0;
                try (var session = getDriver().session()) {
                    for (var o = 0; o < numberOfOperations; ++o) {
                        var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                        var timestamp = store.getLastTimestamp() / 2;
                        for (int s = 0; s < numberOfSnapshots; s++) {
                            var result = session.run(
                                    "CALL time.dynamicNodeSimilarity($startTime, $endTime, $step);",
                                    parameters(START_TIME, timestamp, END_TIME, timestamp, "step", timestamp));
                            if (result.hasNext()) {
                                counter++;
                            }

                            timestamp += step;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runNSIncrementalCallables(
            TemporalStore store, int numberOfOperations, int numberOfSnapshots, int maxSnapshots) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_WORKERS; ++i) {
            callableTasks.add(() -> {
                var counter = 0;
                try (var session = getDriver().session()) {
                    for (var o = 0; o < numberOfOperations; ++o) {
                        var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                        var timestamp = store.getLastTimestamp() / 2;
                        var maxTimestamp = timestamp + step * (numberOfSnapshots - 1);
                        var result = session.run(
                                "CALL time.dynamicNodeSimilarityMutate($startTime, $endTime, $step);",
                                parameters(START_TIME, timestamp, END_TIME, maxTimestamp, "step", step));
                        if (result.hasNext()) {
                            counter++;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runNodeSimilarity(TemporalStore store, String datasetName) {
        var counter = 0;
        var snapshots = new int[] {10, 100};
        var operations = new int[] {ITERATIONS, ITERATIONS};
        var maxSnapshots = Arrays.stream(snapshots).max().getAsInt();

        ResultStore.getInstance().clear();
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var totalOps = ops * NUMBER_OF_WORKERS;
            var startWorkload = System.nanoTime();

            counter += (RUN_NON_INCR) ? runNSCallables(store, ops, numberOfSnapshots, maxSnapshots) : 0;

            var endWorkload = System.nanoTime();
            printResults(datasetName, "NodeSimilarity", totalOps, numberOfSnapshots, startWorkload, endWorkload);
        }

        ResultStore.getInstance().clear();
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var totalOps = ops * NUMBER_OF_WORKERS;
            var startWorkload = System.nanoTime();

            counter += runNSIncrementalCallables(store, ops, numberOfSnapshots, maxSnapshots);

            var endWorkload = System.nanoTime();
            printResults(datasetName, "NodeSimilarityIncr", totalOps, numberOfSnapshots, startWorkload, endWorkload);
        }
        return counter;
    }

    public static int runAvgCallables(
            TemporalStore store, int numberOfOperations, int numberOfSnapshots, int maxSnapshots) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_WORKERS; ++i) {
            callableTasks.add(() -> {
                var counter = 0;
                try (var session = getDriver().session()) {
                    for (var o = 0; o < numberOfOperations; ++o) {
                        var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                        var timestamp = store.getLastTimestamp() / 2;

                        for (int s = 0; s < numberOfSnapshots; s++) {
                            var result = session.run(
                                    "CALL time.avg($property, $timestamp);",
                                    parameters("property", "id", TIMESTAMP, timestamp));
                            if (result.hasNext()) {
                                counter++;
                            }

                            timestamp += step;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runAvgIncrementalCallables(
            TemporalStore store, int numberOfOperations, int numberOfSnapshots, int maxSnapshots) {
        List<Callable<Integer>> callableTasks = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_WORKERS; ++i) {
            callableTasks.add(() -> {
                var counter = 0;
                try (var session = getDriver().session()) {
                    for (var o = 0; o < numberOfOperations; ++o) {
                        var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                        var timestamp = store.getLastTimestamp() / 2;
                        var maxTimestamp = timestamp + step * (numberOfSnapshots - 1);
                        var result = session.run(
                                "CALL time.dynamicAvg($property, $startTime, $endTime, $step);",
                                parameters(
                                        "property", "id", START_TIME, timestamp, END_TIME, maxTimestamp, "step", step));
                        if (result.hasNext()) {
                            counter++;
                        }
                    }
                }
                return counter;
            });
        }
        return runCallables(callableTasks);
    }

    public static int runGlobalAVG(TemporalStore store, String datasetName) {
        var counter = 0;
        var snapshots = new int[] {10, 100};
        var operations = new int[] {ITERATIONS, ITERATIONS};
        var maxSnapshots = Arrays.stream(snapshots).max().getAsInt();

        ResultStore.getInstance().clear();
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var totalOps = ops * NUMBER_OF_WORKERS;
            var startWorkload = System.nanoTime();

            counter += (RUN_NON_INCR) ? runAvgCallables(store, ops, numberOfSnapshots, maxSnapshots) : 0;

            var endWorkload = System.nanoTime();
            printResults(datasetName, "GlobalAvg", totalOps, numberOfSnapshots, startWorkload, endWorkload);
        }

        ResultStore.getInstance().clear();
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var totalOps = ops * NUMBER_OF_WORKERS;
            var startWorkload = System.nanoTime();

            counter += runAvgIncrementalCallables(store, ops, numberOfSnapshots, maxSnapshots);

            var endWorkload = System.nanoTime();
            printResults(datasetName, "GlobalAvgIncr", totalOps, numberOfSnapshots, startWorkload, endWorkload);
        }
        return counter;
    }

    // Method to test the node similarity algorithm from GDS
    public static int runNodeSimilarityProc(TemporalStore store, String datasetName) {
        // Fix HistoryTracker if data is not added with transactions
        var timestamp = store.getLastTimestamp() / 2;

        var startWorkload = System.nanoTime();
        try (var session = getDriver().session()) {
            var result = session.run("CALL time.temporalProjection($timestamp);", parameters(TIMESTAMP, timestamp));
            if (result.hasNext()) {
                result.consume();
            }
        }
        var endWorkload = System.nanoTime();
        printResults(datasetName, "CreateProjection", 1, 1, startWorkload, endWorkload);

        startWorkload = System.nanoTime();
        try (var session = getDriver().session()) {
            var result = session.run(
                    "CALL gds.nodeSimilarity.mutate($graph, { topK: 10, mutateRelationshipType: 'SIMILAR', mutateProperty: 'score' });",
                    parameters(GRAPH_NAME, "neo4jt" + timestamp));
            if (result.hasNext()) {
                result.consume();
            }
        }
        endWorkload = System.nanoTime();
        printResults(datasetName, "NodeSimilarity", 1, 1, startWorkload, endWorkload);

        return 0;
    }

    private static Driver getDriver() {
        return GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4j"));
    }
}
