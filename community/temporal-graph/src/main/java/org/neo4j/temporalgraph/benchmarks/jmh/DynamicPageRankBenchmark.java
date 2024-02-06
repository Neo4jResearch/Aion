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
package org.neo4j.temporalgraph.benchmarks.jmh;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.neo4j.temporalgraph.algorithms.DynamicPageRank;
import org.neo4j.temporalgraph.benchmarks.BenchmarkUtils;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.timeindex.timestore.TimeStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Timeout(time = 600)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Benchmark)
public class DynamicPageRankBenchmark {

    @Param({"100000", "1000000"}) // , "10000000"})
    private int operations;

    @Param({"10", "100", "1000"})
    private int snapshots;

    @Param({"uniform"}) // "zipf"})
    private String distribution;

    private TimeStore store;
    private static final int REL_TYPE = 42;
    private List<InMemoryNode> nodeList;
    private List<InMemoryRelationship> relationshipList;
    private int counter;
    private String debug = null;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
        var numberOfChanges = operations / 10;
        store = BenchmarkUtils.createTimeStore(numberOfChanges, new HashMap<>(), new HashMap<>());

        counter = 0;
        nodeList = BenchmarkUtils.createNodes(operations);
        for (var n : nodeList) {
            store.addUpdate(n);
            tryToCreateSnapshot(numberOfChanges);
        }
        relationshipList = (distribution.equals("zipf"))
                ? BenchmarkUtils.createRelationshipsZipf(operations, REL_TYPE)
                : BenchmarkUtils.createRelationships(operations, REL_TYPE);
        for (var r : relationshipList) {
            store.addUpdate(r);
            tryToCreateSnapshot(numberOfChanges);
        }
        store.takeSnapshot();
    }

    void tryToCreateSnapshot(int numberOfChanges) throws IOException {
        counter++;
        if (counter == numberOfChanges) {
            store.takeSnapshot();
            counter = 0;
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws IOException {
        if (debug != null) {
            System.out.println(debug);
        }
        store.shutdown();
        store = null;
        nodeList = null;
        relationshipList = null;
        BenchmarkUtils.cleanUpFiles();
    }

    @Benchmark
    public void testPageRank(Blackhole blackhole) throws IOException {
        var step = operations / snapshots;
        var timestamp = step;
        for (int i = 0; i < snapshots; i++) {
            // Get and initialize PageRank
            var graph = store.getGraph(timestamp);
            var pagerank = new DynamicPageRank();
            pagerank.initialize(graph);

            // Consume results
            var result = pagerank.getResult();
            blackhole.consume(result);

            timestamp += step;
        }
    }

    @Benchmark
    public void testIncrementalPageRank(Blackhole blackhole) throws IOException {
        var step = operations / snapshots;
        long timestamp = step;

        // Get and initialize PageRank
        var graph = store.getGraph(timestamp);
        var pagerank = new DynamicPageRank();
        pagerank.initialize(graph);

        // Consume results
        var result = pagerank.getResult();
        blackhole.consume(result);

        for (int i = 1; i < snapshots; i++) {
            // Consume updates
            var diff = store.getDiff(timestamp + 1, timestamp + step);
            pagerank.update(diff);

            // Consume results
            result = pagerank.getResult();
            blackhole.consume(result);

            timestamp += step;
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DynamicPageRankBenchmark.class.getSimpleName())
                .jvmArgs("-Xms16g", "-Xmx16g")
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
