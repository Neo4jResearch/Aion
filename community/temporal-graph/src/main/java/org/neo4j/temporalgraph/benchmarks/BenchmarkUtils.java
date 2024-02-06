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

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.config;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.temporalgraph.algorithms.AvgAggregator;
import org.neo4j.temporalgraph.algorithms.DynamicBFS;
import org.neo4j.temporalgraph.algorithms.DynamicCommunityDetection;
import org.neo4j.temporalgraph.algorithms.DynamicGlobalAggregation;
import org.neo4j.temporalgraph.algorithms.DynamicNodeSimilarity;
import org.neo4j.temporalgraph.algorithms.DynamicPageRankV2;
import org.neo4j.temporalgraph.benchmarks.data.GraphLoader;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.lineageindex.entitystores.RelationshipStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.disk.PersistentDoubleListRelationshipStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.disk.PersistentLinkedListRelationshipStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.memory.DoubleListRelationshipStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.memory.LinkedListRelationshipStore;
import org.neo4j.temporalgraph.timeindex.SnapshotCreationPolicy;
import org.neo4j.temporalgraph.timeindex.timestore.TimeStore;
import org.neo4j.temporalgraph.timeindex.timestore.disk.PersistentTimeStore;

public class BenchmarkUtils {
    public static final String DBLP = "DBLP";
    public static final String DBPEDIA = "DBPEDIA";
    public static final String LIVEJOURNAL = "LIVEJOURNAL";
    public static final String ORKUT = "ORKUT";
    public static final String POKEC = "POKEC";
    public static final String WIKITALK = "WIKITALK";
    public static final String WIKIPEDIAFR = "WIKIPEDIAFR";
    public static final String WIKIPEDIAEN = "WIKIPEDIAEN";

    // Bipartite graphs
    public static final String EPINIONS = "EPINIONS";
    public static final String LASTFM = "LASTFM";

    public static final String WIKTIONARY = "WIKTIONARY";
    public static final String WEBTRACKER = "WEBTRACKER";
    public static final String YAHOO = "YAHOO";

    private static final int MB = 1024 * 1024;

    private BenchmarkUtils() {}

    public static RelationshipStore relationshipStoreOf(String type) {
        return switch (type) {
            case "linkedlist" -> new LinkedListRelationshipStore();
            case "doublelist" -> new DoubleListRelationshipStore();
            case "persistentlinkedlist" -> {
                FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                final int pageSize = (int) kibiBytes(16);
                var memoryAllocator = MemoryAllocator.createAllocator(mebiBytes(128), EmptyMemoryTracker.INSTANCE);
                MuninnPageCache.Configuration configuration = config(memoryAllocator)
                        .pageCacheTracer(PageCacheTracer.NULL)
                        .pageSize(pageSize);
                var pageCache = StandalonePageCacheFactory.createPageCache(
                        fs, createInitialisedScheduler(), PageCacheTracer.NULL, configuration);
                Path path = Paths.get("relStore").toAbsolutePath();

                yield new PersistentLinkedListRelationshipStore(pageCache, fs, path, new HashMap<>(), new HashMap<>());
            }
            case "persistentdoublelist" -> {
                FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                final int pageSize = (int) kibiBytes(16);
                var memoryAllocator = MemoryAllocator.createAllocator(mebiBytes(128), EmptyMemoryTracker.INSTANCE);
                MuninnPageCache.Configuration configuration = config(memoryAllocator)
                        .pageCacheTracer(PageCacheTracer.NULL)
                        .pageSize(pageSize);
                var pageCache = StandalonePageCacheFactory.createPageCache(
                        fs, createInitialisedScheduler(), PageCacheTracer.NULL, configuration);
                Path path = Paths.get("relStore").toAbsolutePath();

                yield new PersistentDoubleListRelationshipStore(pageCache, fs, path, new HashMap<>(), new HashMap<>());
            }
            default -> throw new IllegalArgumentException(String.format("Not supported type %s", type));
        };
    }

    public static TimeStore createTimeStore(
            int numberOfChanges, Map<String, Integer> namesToIds, Map<Integer, String> idsToNames) throws IOException {
        var policy = new SnapshotCreationPolicy(numberOfChanges);
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        final int pageSize = (int) kibiBytes(16);
        var memoryAllocator = MemoryAllocator.createAllocator(mebiBytes(128), EmptyMemoryTracker.INSTANCE);
        MuninnPageCache.Configuration configuration =
                config(memoryAllocator).pageCacheTracer(PageCacheTracer.NULL).pageSize(pageSize);
        var pageCache = StandalonePageCacheFactory.createPageCache(
                fs, createInitialisedScheduler(), PageCacheTracer.NULL, configuration);
        return new PersistentTimeStore(
                policy,
                pageCache,
                fs,
                Paths.get("TIMESTORE_LOG").toAbsolutePath(),
                Paths.get("TIMESTORE_INDEX").toAbsolutePath(),
                namesToIds,
                idsToNames);
    }

    public static int randInt(Random rand, int min, int max) {
        return rand.nextInt((max - min) + 1) + min;
    }

    public static List<InMemoryNode> createNodes(int operations) {
        List<InMemoryNode> nodeList = new ArrayList<>();
        for (int i = 0; i < operations; ++i) {
            nodeList.add(new InMemoryNode(i, i));
        }
        return nodeList;
    }

    public static List<InMemoryRelationship> createRelationships(int operations, int type) {
        return createRelationships(operations, operations - 1, type);
    }

    public static List<InMemoryRelationship> createRelationships(int operations, int maxNodeId, int type) {
        List<InMemoryRelationship> relationshipList = new ArrayList<>();
        Random random = new Random(42L);
        for (int i = 0; i < operations; ++i) {
            var startNode = randInt(random, 0, maxNodeId);
            var endNode = randInt(random, 0, maxNodeId);
            var timestamp = randInt(random, Math.max(startNode, endNode), maxNodeId);
            relationshipList.add(new InMemoryRelationship(i, startNode, endNode, type, timestamp));
        }
        relationshipList.sort(Comparator.comparing(InMemoryRelationship::getStartTimestamp));
        return relationshipList;
    }

    public static List<InMemoryRelationship> createRelationshipsZipf(int operations, int type) {
        return createRelationshipsZipf(operations, operations - 1, type);
    }

    public static List<InMemoryRelationship> createRelationshipsZipf(int operations, int maxNodeId, int type) {
        List<InMemoryRelationship> relationshipList = new ArrayList<>();
        var zipf = new ZipfDistribution(maxNodeId, 1);
        Random random = new Random(42L);
        for (int i = 0; i < operations; ++i) {
            var startNode = zipf.sample();
            var endNode = zipf.sample();
            var timestamp = randInt(random, Math.max(startNode, endNode), maxNodeId);
            relationshipList.add(new InMemoryRelationship(i, startNode, endNode, type, timestamp));
        }
        relationshipList.sort(Comparator.comparing(InMemoryRelationship::getStartTimestamp));
        return relationshipList;
    }

    public static void cleanUpFiles() throws IOException {
        Path path = Paths.get("TIMESTORE_LOG").toAbsolutePath();
        Files.deleteIfExists(path);

        path = Paths.get("TIMESTORE_INDEX").toAbsolutePath();
        Files.deleteIfExists(path);

        path = Paths.get("relStore").toAbsolutePath();
        Files.deleteIfExists(path);

        path = Paths.get("inRels").toAbsolutePath();
        Files.deleteIfExists(path);

        path = Paths.get("outRels").toAbsolutePath();
        Files.deleteIfExists(path);

        path = Paths.get("neighbourhoods").toAbsolutePath();
        Files.deleteIfExists(path);
    }

    public static void printResults(
            String datasetName,
            String workloadName,
            int numberOfOperations,
            int counter,
            long startWorkload,
            long endWorkload) {
        long elapsedTimeForWorkloadInNanoseconds = endWorkload - startWorkload;
        double lookupsPerSecond = numberOfOperations / (1e-9 * elapsedTimeForWorkloadInNanoseconds);
        System.out.printf(
                "%s, %s, %d, %d, %d, %f%n",
                datasetName,
                workloadName,
                numberOfOperations,
                counter,
                elapsedTimeForWorkloadInNanoseconds,
                lookupsPerSecond);
    }

    public static int runFetchEntities(TemporalStore store, int numberOfRelationships, int numberOfOperations)
            throws IOException {
        var counter = 0;
        Random random = new Random(42L);
        for (int i = 0; i < numberOfOperations; ++i) {
            var relId = BenchmarkUtils.randInt(random, 0, numberOfRelationships - 1);
            var rel = store.getRelationship(relId, relId);
            if (rel.isPresent()) {
                counter++;
            }
        }
        return counter;
    }

    public static int runFetchSnapshots(TemporalStore store, int numberOfOperations) throws IOException {
        var counter = 0;
        Random random = new Random(42L);
        for (int i = 0; i < numberOfOperations; ++i) {
            var timestamp = BenchmarkUtils.randInt(random, 0, (int) store.getLastTimestamp());
            var snapshot = store.getSnapshot(timestamp);
            counter += snapshot.getNodeMap().size();
        }
        return counter;
    }

    public static void runGlobalAverage(TemporalStore store, String datasetName) throws IOException {
        var snapshots = new int[] {1, 10, 100};
        var operations = new int[] {1, 5, 5};
        var maxSnapshots = Arrays.stream(snapshots).max().getAsInt();

        // First run without incremental execution
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;
                for (int i = 0; i < numberOfSnapshots; i++) {
                    var graph = store.getSnapshot(timestamp);
                    var runningAvg = new DynamicGlobalAggregation(new AvgAggregator(), "id");
                    runningAvg.initialize(graph);

                    // Consume results
                    runningAvg.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "GlobalAvg", ops, numberOfSnapshots, startWorkload, endWorkload);
        }

        // Then run incrementally
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;

                var graph = store.getSnapshot(timestamp);
                var runningAvg = new DynamicGlobalAggregation(new AvgAggregator(), "id");
                runningAvg.initialize(graph);

                // Consume results
                runningAvg.getResult();

                for (int i = 1; i < numberOfSnapshots; i++) {
                    // Consume updates
                    var diff = store.getDiff(timestamp + 1, timestamp + step);
                    runningAvg.update(diff);

                    // Consume results
                    runningAvg.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "GlobalAvgIncr", ops, numberOfSnapshots, startWorkload, endWorkload);
        }
    }

    public static void runBFS(TemporalStore store, String datasetName, int maxNodeId) throws IOException {
        var snapshots = new int[] {1, 10, 100};
        var operations = new int[] {1, 5, 5};
        var maxSnapshots = Arrays.stream(snapshots).max().getAsInt();

        // First run without incremental execution
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            Random random = new Random(42L);
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;
                var sourceId = BenchmarkUtils.randInt(random, 0, maxNodeId - 1);
                for (int i = 0; i < numberOfSnapshots; i++) {
                    var graph = store.getSnapshot(timestamp);
                    var bfs = new DynamicBFS(sourceId);
                    bfs.initialize(graph);

                    // Consume results
                    bfs.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "BFS", ops, numberOfSnapshots, startWorkload, endWorkload);
        }

        // Then run incrementally
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            Random random = new Random(42L);
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;
                var sourceId = BenchmarkUtils.randInt(random, 0, maxNodeId - 1);

                var graph = store.getSnapshot(timestamp);
                var bfs = new DynamicBFS(sourceId);
                bfs.initialize(graph);

                // Consume results
                bfs.getResult();

                for (int i = 1; i < numberOfSnapshots; i++) {
                    // Consume updates
                    var diff = store.getDiff(timestamp + 1, timestamp + step);
                    bfs.update(diff);

                    // Consume results
                    bfs.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "BFSIncr", ops, numberOfSnapshots, startWorkload, endWorkload);
        }
    }

    public static void runPageRank(TemporalStore store, String datasetName) throws IOException {
        var snapshots = new int[] {1, 10, 100};
        var operations = new int[] {1, 3, 3};
        var maxSnapshots = Arrays.stream(snapshots).max().getAsInt();

        // First run without incremental execution
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;
                for (int i = 0; i < numberOfSnapshots; i++) {
                    var graph = store.getSnapshot(timestamp);
                    var pagerank = new DynamicPageRankV2();
                    pagerank.initialize(graph);

                    // Consume results
                    pagerank.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "PageRank", ops, numberOfSnapshots, startWorkload, endWorkload);
        }

        // Then run incrementally
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;

                var graph = store.getSnapshot(timestamp);
                var pagerank = new DynamicPageRankV2();
                pagerank.initialize(graph);

                // Consume results
                pagerank.getResult();

                for (int i = 1; i < numberOfSnapshots; i++) {
                    // Consume updates
                    var diff = store.getDiff(timestamp + 1, timestamp + step);
                    pagerank.update(diff);

                    // Consume results
                    pagerank.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "PageRankIncr", ops, numberOfSnapshots, startWorkload, endWorkload);
        }
    }

    public static void runLabelRank(TemporalStore store, String datasetName) throws IOException {
        var snapshots = new int[] {1, 10, 100};
        var operations = new int[] {1, 1, 1};
        var maxSnapshots = Arrays.stream(snapshots).max().getAsInt();

        // First run without incremental execution
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;
                for (int i = 0; i < numberOfSnapshots; i++) {
                    var graph = store.getSnapshot(timestamp);
                    var labelrank = new DynamicCommunityDetection();
                    labelrank.initialize(graph);

                    // Consume results
                    labelrank.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "LabelRank", ops, numberOfSnapshots, startWorkload, endWorkload);
        }

        // Then run incrementally
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;

                var graph = store.getSnapshot(timestamp);
                var labelrank = new DynamicCommunityDetection();
                labelrank.initialize(graph);

                // Consume results
                labelrank.getResult();

                for (int i = 1; i < numberOfSnapshots; i++) {
                    // Consume updates
                    var diff = store.getDiff(timestamp + 1, timestamp + step);
                    labelrank.update(diff);

                    // Consume results
                    labelrank.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "LabelRankIncr", ops, numberOfSnapshots, startWorkload, endWorkload);
        }
    }

    public static void runNodeSimilarity(TemporalStore store, String datasetName) throws IOException {
        var snapshots = new int[] {1, 10, 100};
        var operations = new int[] {1, 1, 1};
        var maxSnapshots = Arrays.stream(snapshots).max().getAsInt();

        // First run without incremental execution
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;
                for (int i = 0; i < numberOfSnapshots; i++) {
                    var graph = store.getSnapshot(timestamp);
                    var nodeSimilarity = new DynamicNodeSimilarity(true);
                    nodeSimilarity.initialize(graph);

                    // Consume results
                    nodeSimilarity.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "NodeSimilarity", ops, numberOfSnapshots, startWorkload, endWorkload);
        }

        // Then run incrementally
        for (int exp = 0; exp < snapshots.length; ++exp) {
            var numberOfSnapshots = snapshots[exp];
            var ops = operations[exp];
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var step = (store.getLastTimestamp() / 2) / maxSnapshots;
                var timestamp = store.getLastTimestamp() / 2;

                var graph = store.getSnapshot(timestamp);
                var nodeSimilarity = new DynamicNodeSimilarity(true);
                nodeSimilarity.initialize(graph);

                // Consume results
                nodeSimilarity.getResult();

                for (int i = 1; i < numberOfSnapshots; i++) {
                    // Consume updates
                    var diff = store.getDiff(timestamp + 1, timestamp + step);
                    nodeSimilarity.update(diff);

                    // Consume results
                    nodeSimilarity.getResult();

                    timestamp += step;
                }
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "NodeSimilarityIncr", ops, numberOfSnapshots, startWorkload, endWorkload);
        }
    }

    public static void runCreateChains(TemporalStore store, GraphLoader graphLoader, int chain, int maxChain)
            throws IOException {
        var rels = graphLoader.getRelationships();

        var counter = 0;
        var lastTimestamp = rels.get(rels.size() - 1).getStartTimestamp();
        var currentTimestamp = lastTimestamp;
        var numberOfRelationships = graphLoader.numberOfRelationships();
        var updatedRelationships = numberOfRelationships / 8;

        // Set the diff threshold
        store.setDiffThreshold(chain);

        // Add all new data
        for (int c = 0; c < maxChain; c++) {
            currentTimestamp++;
            counter++;
            List<InMemoryRelationship> newRels = new ArrayList<>();
            for (var rel : graphLoader.getRelationships()) {
                var newRel = rel.copy();
                newRel.setStartTimestamp(currentTimestamp);
                newRel.addProperty("Property" + counter, counter);
                newRel.setDiff();
                newRels.add(newRel);

                if (newRels.size() == updatedRelationships) {
                    break;
                }
            }
            store.addRelationships(newRels);
        }

        // Take measurements
        final var operations = 10_000_000;
        runFetchEntities(
                store,
                updatedRelationships,
                lastTimestamp,
                currentTimestamp,
                operations,
                graphLoader.getDatasetName(),
                chain);
        store.flush();
        printStorageOverhead(graphLoader.getDatasetName());
    }

    private static void runFetchEntities(
            TemporalStore store,
            int numberOfRelationships,
            long fromTime,
            long toTime,
            int numberOfOperations,
            String datasetName,
            int diffCounter)
            throws IOException {
        var counter = 0;
        Random random = new Random(42L);

        var startWorkload = System.nanoTime();
        for (int i = 0; i < numberOfOperations; ++i) {
            var relId = BenchmarkUtils.randInt(random, 0, numberOfRelationships - 1);
            var timestamp = toTime; // BenchmarkUtils.randInt(random, (int) fromTime, (int) toTime);
            var rel = store.getRelationship(relId, timestamp);
            if (rel.isPresent()) {
                counter++;
            }
        }
        var endWorkload = System.nanoTime();

        printResults(
                datasetName, "FetchEntities-" + diffCounter, numberOfOperations, counter, startWorkload, endWorkload);
    }

    public static void runExpand(TemporalStore store, String datasetName, int maxNodeId) throws IOException {
        var expands = new int[] {1, 2, 4, 8};
        var operations = new int[] {100, 100, 100, 100};

        // First run with LineageStore
        for (int exp = 0; exp < expands.length; ++exp) {
            var hops = expands[exp];
            var ops = operations[exp];

            var counter = 0;
            Random random = new Random(42L);
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var sourceId = BenchmarkUtils.randInt(random, 0, maxNodeId - 1);
                var timestamp = BenchmarkUtils.randInt(random, 0, (int) store.getLastTimestamp());
                var res = store.expand(sourceId, hops, timestamp, false);
                counter += res.size();
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "LineageStore-expand-" + hops, ops, counter, startWorkload, endWorkload);
        }

        // First run with TimeStore
        for (int exp = 0; exp < expands.length; ++exp) {
            var hops = expands[exp];
            var ops = operations[exp];

            var counter = 0;
            Random random = new Random(42L);
            var startWorkload = System.nanoTime();

            for (var o = 0; o < ops; ++o) {
                var sourceId = BenchmarkUtils.randInt(random, 0, maxNodeId - 1);
                var timestamp = BenchmarkUtils.randInt(random, 0, (int) store.getLastTimestamp());
                var res = store.expand(sourceId, hops, timestamp, true);
                counter += res.size();
            }

            var endWorkload = System.nanoTime();
            printResults(datasetName, "TimeStore-expand-" + hops, ops, counter, startWorkload, endWorkload);
        }
    }

    private static void printStorageOverhead(String datasetName) {
        var dbPath = Paths.get("").toAbsolutePath().resolve("target/neo4j-store-benchmark/data");

        // Print lineage store size
        File relIndexFile = new File(dbPath.toAbsolutePath()
                .resolve("REL_STORE_INDEX")
                .toAbsolutePath()
                .toString());
        File nodeIndexFile = new File(dbPath.toAbsolutePath()
                .resolve("NODE_STORE_INDEX")
                .toAbsolutePath()
                .toString());
        File inRelIndexFile = new File(dbPath.toAbsolutePath()
                .resolve("IN_RELATIONSHIPS")
                .toAbsolutePath()
                .toString());
        File outRelIndexFile = new File(dbPath.toAbsolutePath()
                .resolve("OUT_RELATIONSHIPS")
                .toAbsolutePath()
                .toString());
        var lineageStoreSizeInMb = (int) ((FileUtils.sizeOf(relIndexFile)
                        + FileUtils.sizeOf(nodeIndexFile)
                        + FileUtils.sizeOf(inRelIndexFile)
                        + FileUtils.sizeOf(outRelIndexFile))
                / MB);
        printResults(datasetName, "LineageStoreOverhead", lineageStoreSizeInMb, 0, 1, 2);
    }
}
