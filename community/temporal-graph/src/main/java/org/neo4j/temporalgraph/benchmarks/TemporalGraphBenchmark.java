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
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.EPINIONS;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.LASTFM;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.LIVEJOURNAL;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.ORKUT;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.POKEC;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WEBTRACKER;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKIPEDIAEN;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKIPEDIAFR;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKITALK;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.WIKTIONARY;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.YAHOO;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.printResults;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.runBFS;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.runExpand;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.runFetchEntities;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.runFetchSnapshots;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.runGlobalAverage;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.runNodeSimilarity;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.runPageRank;

import java.io.IOException;
import org.neo4j.temporalgraph.benchmarks.data.DBLPLoader;
import org.neo4j.temporalgraph.benchmarks.data.DBpediaLoader;
import org.neo4j.temporalgraph.benchmarks.data.EpinionsLoader;
import org.neo4j.temporalgraph.benchmarks.data.GraphLoader;
import org.neo4j.temporalgraph.benchmarks.data.LastFmSongsLoader;
import org.neo4j.temporalgraph.benchmarks.data.LiveJournalLoader;
import org.neo4j.temporalgraph.benchmarks.data.OrkutLoader;
import org.neo4j.temporalgraph.benchmarks.data.PokecLoader;
import org.neo4j.temporalgraph.benchmarks.data.WebTrackerLoader;
import org.neo4j.temporalgraph.benchmarks.data.WikiTalkLoader;
import org.neo4j.temporalgraph.benchmarks.data.WikipediaLinkEn;
import org.neo4j.temporalgraph.benchmarks.data.WikipediaLinkFr;
import org.neo4j.temporalgraph.benchmarks.data.WiktionaryLoader;
import org.neo4j.temporalgraph.benchmarks.data.YahooSongsLoader;
import org.neo4j.temporalgraph.utils.DBUtils;

public class TemporalGraphBenchmark {
    private static int numberOfChanges;
    private static int numberOfSnapshots;
    private static boolean RUN_LOOKUP = false;
    private static boolean RUN_EXPAND = false;
    private static boolean RUN_ALGOS = true;
    private static boolean RUN_NODE_SIMILARITY = true;

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf(
                    "Please specify a benchmark: %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, or %s%n"
                            + " and a mode: 0 for lookups, 1 for expansion queries, 2 for algorithms, 3 for bipartite algorithms",
                    DBLP,
                    WEBTRACKER,
                    WIKITALK,
                    EPINIONS,
                    POKEC,
                    LIVEJOURNAL,
                    WIKTIONARY,
                    WIKIPEDIAFR,
                    DBPEDIA,
                    ORKUT,
                    WIKIPEDIAEN);
            return;
        }

        var graphLoader = initializeVariables(args[0]);
        var mode = getArg(args[1]);
        setFlags(mode);

        // Initialize the temporal store
        var store = new TemporalStore(numberOfChanges);

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

        // Import data to the temporal store
        startWorkload = System.nanoTime();
        graphLoader.importGraphToTemporalStore(store);
        endWorkload = System.nanoTime();
        printResults(
                graphLoader.getDatasetName(),
                "ImportToStore",
                numberOfNodes,
                numberOfRelationships,
                startWorkload,
                endWorkload);
        graphLoader.clear();

        // Run lookups
        if (RUN_LOOKUP) {
            // Fetch random relationships
            var operations = 1_000_000;
            startWorkload = System.nanoTime();
            var counter = runFetchEntities(store, numberOfRelationships, operations);
            endWorkload = System.nanoTime();
            printResults(
                    graphLoader.getDatasetName(), "FetchEntities", operations, counter, startWorkload, endWorkload);

            // Fetch random snapshots
            startWorkload = System.nanoTime();
            counter = runFetchSnapshots(store, numberOfSnapshots);
            endWorkload = System.nanoTime();
            printResults(
                    graphLoader.getDatasetName(),
                    "FetchSnapshots",
                    numberOfSnapshots,
                    counter,
                    startWorkload,
                    endWorkload);
        }

        // Run micro-benchmarks
        if (RUN_EXPAND) {
            runExpand(store, graphLoader.getDatasetName(), numberOfNodes);
        }

        // Run algorithms
        if (RUN_ALGOS) {
            if (RUN_NODE_SIMILARITY) {
                runNodeSimilarity(store, graphLoader.getDatasetName());
            } else {
                runGlobalAverage(store, graphLoader.getDatasetName());
                runBFS(store, graphLoader.getDatasetName(), numberOfNodes);
                runPageRank(store, graphLoader.getDatasetName());
                // runLabelRank(store, graphLoader.getDatasetName());
            }
        }

        // Clear the temporal store
        store.clear();
        System.exit(0); // force shutdown -> instead close all thread pools
    }

    private static GraphLoader initializeVariables(String benchmark) {
        switch (benchmark) {
            case DBLP -> {
                numberOfChanges = 500_000;
                numberOfSnapshots = 100;
                DBUtils.nodeCapacity = 524288;
                DBUtils.relCapacity = 2 * 1048576;
                return new DBLPLoader(DBLPLoader.DEFAULT_PATH);
            }
            case WIKITALK -> {
                numberOfChanges = 2_000_000;
                numberOfSnapshots = 100;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 2 * 1048576;
                return new WikiTalkLoader(WikiTalkLoader.DEFAULT_PATH);
            }
            case EPINIONS -> {
                numberOfChanges = 3_000_000;
                numberOfSnapshots = 100;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 4 * 1048576;
                return new EpinionsLoader(EpinionsLoader.DEFAULT_PATH);
            }
            case LASTFM -> {
                numberOfChanges = 3_000_000;
                numberOfSnapshots = 100;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 10 * 1048576;
                return new LastFmSongsLoader(LastFmSongsLoader.DEFAULT_PATH);
            }
            case POKEC -> {
                numberOfChanges = 12_000_000;
                numberOfSnapshots = 10;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 16 * 1048576;
                return new PokecLoader(PokecLoader.DEFAULT_PATH);
            }
            case WIKTIONARY -> {
                numberOfChanges = 16_000_000;
                numberOfSnapshots = 10;
                DBUtils.nodeCapacity = 1048576;
                DBUtils.relCapacity = 32 * 1048576;
                return new WiktionaryLoader(WiktionaryLoader.DEFAULT_PATH);
            }
            case LIVEJOURNAL -> {
                numberOfChanges = 38_000_000;
                numberOfSnapshots = 5;
                DBUtils.nodeCapacity = 8 * 1048576;
                DBUtils.relCapacity = 128 * 1048576;
                return new LiveJournalLoader(LiveJournalLoader.DEFAULT_PATH);
            }
            case WEBTRACKER -> {
                numberOfChanges = 0; // don't store snapshots
                numberOfSnapshots = 5;
                DBUtils.nodeCapacity = 4 * 1048576;
                DBUtils.relCapacity = 256 * 1048576;
                return new WebTrackerLoader(WebTrackerLoader.DEFAULT_PATH);
            }
            case WIKIPEDIAFR -> {
                numberOfChanges = 0; // don't store snapshots
                numberOfSnapshots = 5;
                DBUtils.nodeCapacity = 4 * 1048576;
                DBUtils.relCapacity = 128 * 1048576;
                return new WikipediaLinkFr(WikipediaLinkFr.DEFAULT_PATH);
            }
            case DBPEDIA -> {
                numberOfChanges = 0; // don't store snapshots
                numberOfSnapshots = 5;
                DBUtils.nodeCapacity = 32 * 1048576;
                DBUtils.relCapacity = 256 * 1048576;
                return new DBpediaLoader(DBpediaLoader.DEFAULT_PATH);
            }
            case ORKUT -> {
                numberOfChanges = 0; // don't store snapshots
                numberOfSnapshots = 5;
                DBUtils.nodeCapacity = 4 * 1048576;
                DBUtils.relCapacity = 256 * 1048576;
                return new OrkutLoader(OrkutLoader.DEFAULT_PATH);
            }
            case YAHOO -> {
                numberOfChanges = 38_000_000;
                numberOfSnapshots = 5;
                DBUtils.nodeCapacity = 8 * 1048576;
                DBUtils.relCapacity = 256 * 1048576;
                return new YahooSongsLoader(YahooSongsLoader.DEFAULT_PATH);
            }
            case WIKIPEDIAEN -> {
                numberOfChanges = 0; // don't store snapshots
                numberOfSnapshots = 5;
                DBUtils.nodeCapacity = 16 * 1048576;
                DBUtils.relCapacity = 512 * 1048576;
                return new WikipediaLinkEn(WikipediaLinkEn.DEFAULT_PATH);
            }
            default -> throw new IllegalArgumentException(String.format("Unknown benchmark %s", benchmark));
        }
    }

    private static int getArg(String arg) {
        int res = Integer.parseInt(arg);
        if (res >= 0 && res < 4) {
            return res;
        }
        throw new IllegalArgumentException(String.format("Invalid arg %d", res));
    }

    private static void setFlags(int mode) {
        if (mode == 0) {
            RUN_LOOKUP = true;
            RUN_EXPAND = false;
            RUN_ALGOS = false;
            RUN_NODE_SIMILARITY = false;
        } else if (mode == 1) {
            RUN_LOOKUP = false;
            RUN_EXPAND = true;
            RUN_ALGOS = false;
            RUN_NODE_SIMILARITY = false;
        } else if (mode == 2) {
            RUN_LOOKUP = false;
            RUN_EXPAND = false;
            RUN_ALGOS = true;
            RUN_NODE_SIMILARITY = false;
        } else {
            RUN_LOOKUP = false;
            RUN_EXPAND = false;
            RUN_ALGOS = true;
            RUN_NODE_SIMILARITY = true;
        }
    }
}
