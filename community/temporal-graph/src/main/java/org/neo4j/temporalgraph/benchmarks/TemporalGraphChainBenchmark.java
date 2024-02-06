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
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.printResults;
import static org.neo4j.temporalgraph.benchmarks.BenchmarkUtils.runCreateChains;

import java.io.IOException;
import org.neo4j.temporalgraph.benchmarks.data.DBLPLoader;
import org.neo4j.temporalgraph.benchmarks.data.GraphLoader;
import org.neo4j.temporalgraph.utils.DBUtils;

public class TemporalGraphChainBenchmark {
    private static int numberOfChanges;

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf(
                    "Please specify a benchmark: %s, %s, %s, %s, %s, %s, %s, or %s%n",
                    DBLP, WIKITALK, POKEC, LIVEJOURNAL, WIKIPEDIAFR, DBPEDIA, ORKUT, WIKIPEDIAEN);
            return;
        }

        var graphLoader = initializeVariables(args[0]);
        var chain = Integer.parseInt(args[1]);
        final var maxChain = 31;
        if (chain > maxChain + 2) {
            throw new IllegalArgumentException(String.format("Invalid chain parameter: %d > %d + 2", chain, maxChain));
        }

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

        // Initialize the temporal store
        var store = new TemporalStore(numberOfChanges);

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

        runCreateChains(store, graphLoader, chain, maxChain);

        // Clear the temporal store
        store.clear();
        System.exit(0); // force shutdown -> instead close all thread pools
    }

    private static GraphLoader initializeVariables(String benchmark) {
        if (benchmark.equals(DBLP)) {
            numberOfChanges = 0;
            DBUtils.nodeCapacity = 524288;
            DBUtils.relCapacity = 16 * 1048576;
            return new DBLPLoader(DBLPLoader.DEFAULT_PATH);
        }
        throw new IllegalArgumentException(String.format("Unknown benchmark %s", benchmark));
    }
}
