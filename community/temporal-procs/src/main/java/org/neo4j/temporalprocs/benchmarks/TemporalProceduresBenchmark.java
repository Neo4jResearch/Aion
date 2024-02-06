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
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.DBLP;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.DBPEDIA;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.EPINIONS;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.LASTFM;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.LIVEJOURNAL;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.ORKUT;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.POKEC;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKIPEDIAEN;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKIPEDIAFR;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKITALK;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKTIONARY;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.printResults;
import static org.neo4j.temporalprocs.benchmarks.BenchmarkUtils.runBFS;
import static org.neo4j.temporalprocs.benchmarks.BenchmarkUtils.runFetchAllRelationships;
import static org.neo4j.temporalprocs.benchmarks.BenchmarkUtils.runFetchAndWriteEntities;
import static org.neo4j.temporalprocs.benchmarks.BenchmarkUtils.runFetchEntities;
import static org.neo4j.temporalprocs.benchmarks.BenchmarkUtils.runGlobalAVG;
import static org.neo4j.temporalprocs.benchmarks.BenchmarkUtils.runNodeSimilarity;
import static org.neo4j.temporalprocs.benchmarks.BenchmarkUtils.runPageRank;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Settings;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamProc;
import org.neo4j.gds.pagerank.PageRankStreamProc;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateProc;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.temporalgraph.benchmarks.TemporalStore;
import org.neo4j.temporalgraph.benchmarks.data.DBLPLoader;
import org.neo4j.temporalgraph.benchmarks.data.DBpediaLoader;
import org.neo4j.temporalgraph.benchmarks.data.EpinionsLoader;
import org.neo4j.temporalgraph.benchmarks.data.GraphLoader;
import org.neo4j.temporalgraph.benchmarks.data.LastFmSongsLoader;
import org.neo4j.temporalgraph.benchmarks.data.LiveJournalLoader;
import org.neo4j.temporalgraph.benchmarks.data.OrkutLoader;
import org.neo4j.temporalgraph.benchmarks.data.PokecLoader;
import org.neo4j.temporalgraph.benchmarks.data.WikiTalkLoader;
import org.neo4j.temporalgraph.benchmarks.data.WikipediaLinkEn;
import org.neo4j.temporalgraph.benchmarks.data.WikipediaLinkFr;
import org.neo4j.temporalgraph.benchmarks.data.WiktionaryLoader;
import org.neo4j.temporalgraph.lineageindex.EntityLineageTracker;
import org.neo4j.temporalgraph.lineageindex.PersistentLineageStore;
import org.neo4j.temporalgraph.timeindex.SnapshotCreationPolicy;
import org.neo4j.temporalgraph.timeindex.timestore.TimeBasedTracker;
import org.neo4j.temporalgraph.timeindex.timestore.disk.PersistentTimeStore;
import org.neo4j.temporalgraph.utils.DBUtils;
import org.neo4j.temporalprocs.LineageStoreProcedures;
import org.neo4j.temporalprocs.TimeStoreProcedures;

public class TemporalProceduresBenchmark {
    private static final Path DB_PATH = Path.of("target/neo4j-store-benchmark");
    private static Neo4j db;
    private static EntityLineageTracker lineageTracker = null;
    private static TimeBasedTracker timeBasedTracker = null;
    private static int numberOfChanges;
    private static int numberOfSnapshots;
    private static final boolean RETAIN_DATA = false;
    private static final boolean RUN_FETCH_ENTITIES = true;
    private static final boolean RUN_FETCH_ENTITIES_WITH_WRITES = true;
    private static final boolean RUN_FETCH_SNAPSHOTS = false;
    private static final boolean RUN_ALGOS = false;
    private static boolean RUN_NODE_SIMILARITY = false;
    private static final int MB = 1024 * 1024;

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.printf(
                    "Please specify a benchmark (%s, %s, %s, %s, %s, %s, %s, %s, %s, or %s), %n"
                            + "a mode (0 for benchmark execution, "
                            + "1 for data loading with temporal indexing, "
                            + "2 for data loading without temporal indexing), %n"
                            + "and which store(s) to use (0 for both, 1 for LineageStore, 2 for TimeStore).",
                    DBLP, WIKITALK, EPINIONS, POKEC, WIKTIONARY, LIVEJOURNAL, WIKIPEDIAFR, DBPEDIA, ORKUT, WIKIPEDIAEN);
            return;
        }

        var graphLoader = initializeVariables(args[0]);
        var mode = getArg(args[1]);
        var storeType = getArg(args[2]);
        createDB(mode, storeType);

        // Initialize the temporal store
        var store = (mode != 2)
                ? new TemporalStore(
                        numberOfChanges,
                        db.config().get(GraphDatabaseSettings.neo4j_home),
                        (PersistentLineageStore) lineageTracker.getLineageStore(),
                        (PersistentTimeStore) timeBasedTracker.getTimeStore())
                : null;

        // Load data from disk
        var startWorkload = System.nanoTime();
        graphLoader.loadGraphFromDisk();
        var endWorkload = System.nanoTime();
        var numberOfNodes = graphLoader.numberOfNodes();
        var numberOfRelationships = graphLoader.numberOfRelationships();
        printResults(
                graphLoader.getDatasetName(),
                "LoadFromDisk",
                numberOfNodes,
                numberOfRelationships,
                startWorkload,
                endWorkload);

        // Import data to the store
        importDataToStore(graphLoader, store, mode, numberOfNodes, numberOfRelationships);

        if (mode == 0) {
            // Fetch random relationships
            var operations = 100_000 * WorkerThreadPool.THREAD_NUMBER;
            startWorkload = System.nanoTime();
            var counter = (RUN_FETCH_ENTITIES)
                    ? ((RUN_FETCH_ENTITIES_WITH_WRITES)
                            ? runFetchAndWriteEntities(numberOfNodes, numberOfRelationships, operations)
                            : runFetchEntities(numberOfRelationships, operations))
                    : 0;
            endWorkload = System.nanoTime();
            printResults(
                    graphLoader.getDatasetName(), "FetchEntities", operations, counter, startWorkload, endWorkload);

            // Fetch random snapshots
            startWorkload = System.nanoTime();
            counter = (RUN_FETCH_SNAPSHOTS) ? runFetchAllRelationships(store, numberOfSnapshots) : 0;
            endWorkload = System.nanoTime();
            printResults(
                    graphLoader.getDatasetName(),
                    "FetchAllRelationships",
                    numberOfSnapshots,
                    counter,
                    startWorkload,
                    endWorkload);

            // Run algorithms
            if (RUN_ALGOS) {
                if (RUN_NODE_SIMILARITY) {
                    runNodeSimilarity(store, graphLoader.getDatasetName());
                } else {
                    runGlobalAVG(store, graphLoader.getDatasetName());
                    runBFS(store, graphLoader.getDatasetName(), numberOfNodes);
                    runPageRank(store, graphLoader.getDatasetName());
                }
            }
        }

        // Clear the temporal store
        if (store != null && !RETAIN_DATA) {
            store.clear();
        }
        System.exit(0); // force shutdown -> instead close all thread pools
    }

    private static GraphLoader initializeVariables(String benchmark) {
        switch (benchmark) {
            case DBLP -> {
                numberOfChanges = 500_000;
                numberOfSnapshots = 20 * WorkerThreadPool.THREAD_NUMBER;
                DBUtils.nodeCapacity = 524288;
                DBUtils.relCapacity = 2 * 1048576;
                return new DBLPLoader(DBLPLoader.DEFAULT_PATH);
            }
            case WIKITALK -> {
                numberOfChanges = 2_000_000;
                numberOfSnapshots = 10 * WorkerThreadPool.THREAD_NUMBER;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 2 * 1048576;
                return new WikiTalkLoader(WikiTalkLoader.DEFAULT_PATH);
            }
            case EPINIONS -> {
                RUN_NODE_SIMILARITY = true;
                numberOfChanges = 3_000_000;
                numberOfSnapshots = 100;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 4 * 1048576;
                return new EpinionsLoader(EpinionsLoader.DEFAULT_PATH);
            }
            case LASTFM -> {
                RUN_NODE_SIMILARITY = true;
                numberOfChanges = 3_000_000;
                numberOfSnapshots = 100;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 10 * 1048576;
                return new LastFmSongsLoader(LastFmSongsLoader.DEFAULT_PATH);
            }
            case POKEC -> {
                numberOfChanges = 12_000_000;
                numberOfSnapshots = 5 * WorkerThreadPool.THREAD_NUMBER;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 16 * 1048576;
                return new PokecLoader(PokecLoader.DEFAULT_PATH);
            }
            case WIKTIONARY -> {
                RUN_NODE_SIMILARITY = true;
                numberOfChanges = 16_000_000;
                numberOfSnapshots = 10;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 32 * 1048576;
                return new WiktionaryLoader(WiktionaryLoader.DEFAULT_PATH);
            }
            case LIVEJOURNAL -> {
                numberOfChanges = 38_000_000;
                numberOfSnapshots = 5 * WorkerThreadPool.THREAD_NUMBER;
                DBUtils.nodeCapacity = 8 * 1048576;
                DBUtils.relCapacity = 128 * 1048576;
                return new LiveJournalLoader(LiveJournalLoader.DEFAULT_PATH);
            }
            case WIKIPEDIAFR -> {
                numberOfChanges = 0; // don't store snapshots
                numberOfSnapshots = 5 * WorkerThreadPool.THREAD_NUMBER;
                DBUtils.nodeCapacity = 4 * 1048576;
                DBUtils.relCapacity = 128 * 1048576;
                return new WikipediaLinkFr(WikipediaLinkFr.DEFAULT_PATH);
            }
            case DBPEDIA -> {
                numberOfChanges = 0; // don't store snapshots
                numberOfSnapshots = 5 * WorkerThreadPool.THREAD_NUMBER;
                DBUtils.nodeCapacity = 32 * 1048576;
                DBUtils.relCapacity = 256 * 1048576;
                return new DBpediaLoader(DBpediaLoader.DEFAULT_PATH);
            }
            case ORKUT -> {
                numberOfChanges = 0; // don't store snapshots
                numberOfSnapshots = 5 * WorkerThreadPool.THREAD_NUMBER;
                DBUtils.nodeCapacity = 4 * 1048576;
                DBUtils.relCapacity = 256 * 1048576;
                return new OrkutLoader(OrkutLoader.DEFAULT_PATH);
            }
            case WIKIPEDIAEN -> {
                numberOfChanges = 0; // don't store snapshots
                numberOfSnapshots = 5 * WorkerThreadPool.THREAD_NUMBER;
                DBUtils.nodeCapacity = 16 * 1048576;
                DBUtils.relCapacity = 512 * 1048576;
                return new WikipediaLinkEn(WikipediaLinkEn.DEFAULT_PATH);
            }
            default -> throw new IllegalArgumentException(String.format("Unknown benchmark %s", benchmark));
        }
    }

    private static void createDB(int mode, int storeType) throws IOException {
        db = Neo4jBuilders.newInProcessBuilder(DB_PATH)
                .withConfig(Settings.procedureUnrestricted(), List.of("gds.*"))
                .withConfig(GraphDatabaseSettings.procedure_allowlist, List.of("gds.*"))
                .withConfig(pagecache_memory, ByteUnit.gibiBytes(32))
                .withConfig(BoltConnector.listen_address, new SocketAddress("localhost", 7687))
                .withConfig(BoltConnector.enabled, true)
                .withProcedure(GraphProjectProc.class)
                .withProcedure(PageRankStreamProc.class)
                .withProcedure(LabelPropagationStreamProc.class)
                .withProcedure(NodeSimilarityMutateProc.class)
                .withProcedure(LineageStoreProcedures.class)
                .withProcedure(TimeStoreProcedures.class)
                .build();

        if (mode != 2) {
            var dbms = db.databaseManagementService();
            var pageCache = (PageCache) dbms.database(DEFAULT_DATABASE_NAME).getPageCache();
            var fs =
                    (FileSystemAbstraction) dbms.database(DEFAULT_DATABASE_NAME).getFileSystem();

            var dbPath = db.config().get(GraphDatabaseSettings.neo4j_home);
            var temporalPath = dbPath.toAbsolutePath().resolve("data/temporal");
            Files.createDirectories(temporalPath);

            var nodeIndexPath = temporalPath.toAbsolutePath().resolve("NODE_STORE_INDEX");
            var relIndexPath = temporalPath.toAbsolutePath().resolve("REL_STORE_INDEX");
            lineageTracker = new EntityLineageTracker(pageCache, fs, nodeIndexPath, relIndexPath);
            if (storeType == 0 || storeType == 1) {
                dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, lineageTracker);
            }

            var policy = new SnapshotCreationPolicy(numberOfChanges);
            var logPath = temporalPath.toAbsolutePath().resolve("DATA_LOG");
            var timeIndexPath = temporalPath.toAbsolutePath().resolve("TIME_INDEX");
            timeBasedTracker = new TimeBasedTracker(policy, pageCache, fs, logPath, timeIndexPath);
            if (storeType == 0 || storeType == 2) {
                dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, timeBasedTracker);
            }
        }
    }

    private static int getArg(String arg) {
        int res = Integer.parseInt(arg);
        if (res >= 0 && res < 3) {
            return res;
        }
        throw new IllegalArgumentException(String.format("Invalid arg %d", res));
    }

    private static void importDataToStore(
            GraphLoader graphLoader, TemporalStore store, int mode, int numberOfNodes, int numberOfRelationships)
            throws IOException {
        var workloadName = "ImportToStore";
        var startWorkload = System.nanoTime();
        if (mode == 0) {
            graphLoader.importGraphToTemporalStore(store);
        } else {
            workloadName += (mode == 1) ? "TemporalDB" : "DB";
            BenchmarkUtils.importGraphToDB(graphLoader, db, store);
        }
        var endWorkload = System.nanoTime();
        printResults(
                graphLoader.getDatasetName(),
                workloadName,
                numberOfNodes,
                numberOfRelationships,
                startWorkload,
                endWorkload);
        graphLoader.clear();

        if (mode == 1) {
            printStorageOverhead(graphLoader.getDatasetName());
        }
    }

    private static void printStorageOverhead(String datasetName) {
        // Measure db size
        var dbPath = db.config().get(GraphDatabaseSettings.neo4j_home);
        File dataFolder = new File(dbPath.toAbsolutePath().resolve("data").toString());
        var dbSizeInMB = (int) (FileUtils.sizeOfDirectory(dataFolder) / MB);

        // Measure temporal data size
        File temporalFolder =
                new File(dbPath.toAbsolutePath().resolve("data/temporal").toString());
        var temporalSizeInMB = (int) (FileUtils.sizeOfDirectory(temporalFolder) / MB);

        // Print db size vs temporal data size
        printResults(datasetName, "StorageOverhead", dbSizeInMB, temporalSizeInMB, 1, 2);

        // Print lineage store size
        File relIndexFile = new File(
                dbPath.toAbsolutePath().resolve("data/temporal/REL_STORE_INDEX").toString());
        File nodeIndexFile = new File(dbPath.toAbsolutePath()
                .resolve("data/temporal/NODE_STORE_INDEX")
                .toString());
        File inRelIndexFile = new File(dbPath.toAbsolutePath()
                .resolve("data/temporal/IN_RELATIONSHIPS")
                .toString());
        File outRelIndexFile = new File(dbPath.toAbsolutePath()
                .resolve("data/temporal/OUT_RELATIONSHIPS")
                .toString());
        var lineageStoreSizeInMb = (int) ((FileUtils.sizeOf(relIndexFile)
                        + FileUtils.sizeOf(nodeIndexFile)
                        + FileUtils.sizeOf(inRelIndexFile)
                        + FileUtils.sizeOf(outRelIndexFile))
                / MB);
        printResults(datasetName, "LineageStoreOverhead", lineageStoreSizeInMb, 0, 1, 2);

        // Print time store size
        File dataLogFile = new File(
                dbPath.toAbsolutePath().resolve("data/temporal/DATA_LOG").toString());
        File timeIndexFile = new File(
                dbPath.toAbsolutePath().resolve("data/temporal/TIME_INDEX").toString());
        File snapshotIndexFile = new File(
                dbPath.toAbsolutePath().resolve("data/temporal/SNAPSHOT_INDEX").toString());
        var timeStoreSizeInMb = (int)
                ((FileUtils.sizeOf(dataLogFile) + FileUtils.sizeOf(timeIndexFile) + FileUtils.sizeOf(snapshotIndexFile))
                        / MB);
        var snapshotsSizeInMB = temporalSizeInMB - (timeStoreSizeInMb + lineageStoreSizeInMb);
        timeStoreSizeInMb += snapshotsSizeInMB;
        printResults(datasetName, "TimeStoreOverhead", timeStoreSizeInMb, snapshotsSizeInMB, 1, 2);
    }
}
