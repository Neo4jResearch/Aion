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
package org.neo4j.temporalgraph.benchmarks;

import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.DBLP;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.DBPEDIA;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.LIVEJOURNAL;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.ORKUT;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.POKEC;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKIPEDIAEN;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKIPEDIAFR;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKITALK;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.cleanUpFiles;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.neo4j.temporalgraph.benchmarks.data.DBLPLoader;
import org.neo4j.temporalgraph.benchmarks.data.DBpediaLoader;
import org.neo4j.temporalgraph.benchmarks.data.GraphLoader;
import org.neo4j.temporalgraph.benchmarks.data.LiveJournalLoader;
import org.neo4j.temporalgraph.benchmarks.data.OrkutLoader;
import org.neo4j.temporalgraph.benchmarks.data.PokecLoader;
import org.neo4j.temporalgraph.benchmarks.data.WikiTalkLoader;
import org.neo4j.temporalgraph.benchmarks.data.WikipediaLinkEn;
import org.neo4j.temporalgraph.benchmarks.data.WikipediaLinkFr;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.entitystores.RelationshipStore;

public class RelationshipStoreBenchmark {
    private static final String LINKED_LIST_BENCHMARK_COMMAND = "linkedlist";
    private static final String DOUBLE_LIST_BENCHMARK_COMMAND = "doublelist";
    private static final String PERSISTENT_LINKED_LIST_BENCHMARK_COMMAND = "persistentlinkedlist";
    private static final String PERSISTENT_DOUBLE_LIST_BENCHMARK_COMMAND = "persistentdoublelist";
    private static int NUMBER_OF_RELATIONSHIPS = 10_000_000;
    private static final int NUMBER_OF_OPERATIONS = 1_000_000;
    private static final boolean isZipf = true;
    private static final boolean testsEnabled = false;

    static int loadData(RelationshipStore store, List<InMemoryRelationship> inputRels) throws IOException {
        store.addRelationships(inputRels);
        return store.numberOfNeighbourhoodRecords();
    }

    static void runTests(RelationshipStore store, List<InMemoryRelationship> inputRels) throws IOException {
        System.out.println("Running tests...");

        var testStore = BenchmarkUtils.relationshipStoreOf(DOUBLE_LIST_BENCHMARK_COMMAND);
        testStore.addRelationships(inputRels);

        if (testStore.numberOfRelationships() != store.numberOfRelationships()
                || testStore.numberOfNeighbourhoodRecords() != store.numberOfNeighbourhoodRecords()) {
            // throw new RuntimeException(
            System.out.printf(
                    "The store internals must match: %d #rels, %d #neighbourhoods, %d #rels, %d #neighbourhoods%n",
                    testStore.numberOfRelationships(),
                    testStore.numberOfNeighbourhoodRecords(),
                    store.numberOfRelationships(),
                    store.numberOfNeighbourhoodRecords());
        }

        for (int relId = 0; relId < NUMBER_OF_RELATIONSHIPS; ++relId) {
            var rel1 = testStore.getRelationship(relId, relId);
            var rel2 = store.getRelationship(relId, relId);
            if (!rel1.get().equalsWithoutPointers(rel2.get())) {
                throw new RuntimeException(
                        String.format("The store internals must match: %s is not equal to %s", rel1, rel2));
            }
        }

        final var timestamp = NUMBER_OF_RELATIONSHIPS;
        for (int nodeId = 0; nodeId < NUMBER_OF_RELATIONSHIPS; ++nodeId) {
            var rels1 = testStore.getRelationships(nodeId, RelationshipDirection.BOTH, timestamp);
            if (!rels1.isEmpty()) {
                rels1.sort(Comparator.comparing(InMemoryRelationship::getEntityId));
            }

            var rels2 = store.getRelationships(nodeId, RelationshipDirection.BOTH, timestamp);
            if (!rels2.isEmpty()) {
                rels2.sort(Comparator.comparing(InMemoryRelationship::getEntityId));
            }

            if (rels1.size() != rels2.size()) {
                throw new RuntimeException(
                        String.format("The store internals must match: %s is not equal to %s", rels1, rels2));
            }
            for (int i = 0; i < rels1.size(); i++) {
                if (!rels1.get(i).equalsWithoutPointers(rels2.get(i))) {
                    throw new RuntimeException(
                            String.format("The store internals must match: %s is not equal to %s", rels1, rels2));
                }
            }
        }

        System.out.println("Tests completed...");
    }

    static int runGetRelationshipWorkload(RelationshipStore store, int numberOfOperations) throws IOException {
        Random random = new Random(42L);
        var counter = 0;
        for (int i = 0; i < numberOfOperations; ++i) {
            final var relId = BenchmarkUtils.randInt(random, 0, NUMBER_OF_RELATIONSHIPS - 1);
            final var timestamp = relId;
            var rel = store.getRelationship(relId, timestamp);
            if (rel.isPresent()) {
                counter++;
            }
        }
        return counter;
    }

    static int runAllRelationshipsWorkload(RelationshipStore store, int numberOfOperations) throws IOException {
        Random random = new Random(42L);
        var counter = 0;
        for (int i = 0; i < numberOfOperations; ++i) {
            var timestamp = BenchmarkUtils.randInt(random, 0, NUMBER_OF_RELATIONSHIPS - 1);
            var rels = store.getAllRelationships(timestamp);
            counter += rels.size();
        }
        return counter;
    }

    static int runNodeRelationshipsWorkload(RelationshipStore store, int numberOfOperations) throws IOException {
        Random random = new Random(42L);
        var counter = 0;
        for (int i = 0; i < numberOfOperations; ++i) {
            var nodeId = BenchmarkUtils.randInt(random, 0, NUMBER_OF_RELATIONSHIPS - 1);
            var timestamp = BenchmarkUtils.randInt(random, 0, NUMBER_OF_RELATIONSHIPS - 1);
            var rels = store.getRelationships(nodeId, RelationshipDirection.BOTH, timestamp);
            counter += rels.size();
        }
        return counter;
    }

    static int deleteData(RelationshipStore store, List<InMemoryRelationship> inputRels) throws IOException {
        for (var r : inputRels) {
            r.setDeleted();
            store.addRelationship(r);
        }
        return store.numberOfNeighbourhoodRecords();
    }

    static void printResults(
            String workloadName,
            String storeName,
            int numberOfOperations,
            long startWorkload,
            long endWorkload,
            int counter) {
        long elapsedTimeForWorkloadInNanoseconds = endWorkload - startWorkload;
        double lookupsPerSecond = numberOfOperations / (1e-9 * elapsedTimeForWorkloadInNanoseconds);
        System.out.printf(
                "%s, %s, %s, %d, %d, %d, %f%n",
                workloadName,
                storeName,
                (isZipf) ? "zipf" : "uniform",
                numberOfOperations,
                counter,
                elapsedTimeForWorkloadInNanoseconds,
                lookupsPerSecond);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf(
                    "Please specify a command: %s, %s, %s, or %s%n",
                    LINKED_LIST_BENCHMARK_COMMAND,
                    DOUBLE_LIST_BENCHMARK_COMMAND,
                    PERSISTENT_LINKED_LIST_BENCHMARK_COMMAND,
                    PERSISTENT_DOUBLE_LIST_BENCHMARK_COMMAND);
            return;
        }

        cleanUpFiles();

        var graphLoader = initializeVariables(args[0]);
        graphLoader.loadGraphFromDisk();
        NUMBER_OF_RELATIONSHIPS = graphLoader.numberOfRelationships();
        String command = args[1];
        var store = BenchmarkUtils.relationshipStoreOf(command);

        // Load data
        var inputRels = graphLoader.getRelationships();
        // isZipf ? BenchmarkUtils.createRelationshipsZipf(NUMBER_OF_RELATIONSHIPS, 42)
        // : BenchmarkUtils.createRelationships(NUMBER_OF_RELATIONSHIPS, 42);
        long startWorkload = System.nanoTime();
        var counter = loadData(store, inputRels);
        long endWorkload = System.nanoTime();
        printResults("loadData", command, NUMBER_OF_OPERATIONS, startWorkload, endWorkload, counter);

        // Test for correctness
        if (testsEnabled) {
            runTests(store, inputRels);
        }

        // Run first workload
        startWorkload = System.nanoTime();
        counter = runGetRelationshipWorkload(store, NUMBER_OF_OPERATIONS);
        endWorkload = System.nanoTime();
        printResults("runGetRelationshipWorkload", command, NUMBER_OF_OPERATIONS, startWorkload, endWorkload, counter);

        // Run second workload
        startWorkload = System.nanoTime();
        counter = runNodeRelationshipsWorkload(store, NUMBER_OF_OPERATIONS);
        endWorkload = System.nanoTime();
        printResults(
                "runNodeRelationshipsWorkload", command, NUMBER_OF_OPERATIONS, startWorkload, endWorkload, counter);

        // Change the number of operations
        final var numberOfOperations = 100;

        // Run third workload
        startWorkload = System.nanoTime();
        counter = runAllRelationshipsWorkload(store, numberOfOperations);
        endWorkload = System.nanoTime();
        printResults("runAllRelationshipsWorkload", command, numberOfOperations, startWorkload, endWorkload, counter);

        // Delete data
        startWorkload = System.nanoTime();
        counter = deleteData(store, inputRels);
        endWorkload = System.nanoTime();
        printResults("deleteData", command, NUMBER_OF_OPERATIONS, startWorkload, endWorkload, counter);

        cleanUpFiles();
    }

    private static GraphLoader initializeVariables(String benchmark) {
        switch (benchmark) {
            case DBLP -> {
                return new DBLPLoader(DBLPLoader.DEFAULT_PATH);
            }
            case WIKITALK -> {
                return new WikiTalkLoader(WikiTalkLoader.DEFAULT_PATH);
            }
            case POKEC -> {
                return new PokecLoader(PokecLoader.DEFAULT_PATH);
            }
            case LIVEJOURNAL -> {
                return new LiveJournalLoader(LiveJournalLoader.DEFAULT_PATH);
            }
            case WIKIPEDIAFR -> {
                return new WikipediaLinkFr(WikipediaLinkFr.DEFAULT_PATH);
            }
            case DBPEDIA -> {
                return new DBpediaLoader(DBpediaLoader.DEFAULT_PATH);
            }
            case ORKUT -> {
                return new OrkutLoader(OrkutLoader.DEFAULT_PATH);
            }
            case WIKIPEDIAEN -> {
                return new WikipediaLinkEn(WikipediaLinkEn.DEFAULT_PATH);
            }
            default -> throw new IllegalArgumentException(String.format("Unknown benchmark %s", benchmark));
        }
    }
}
