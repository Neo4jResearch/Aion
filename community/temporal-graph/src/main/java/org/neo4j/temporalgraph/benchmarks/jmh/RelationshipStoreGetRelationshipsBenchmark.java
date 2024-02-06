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
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.neo4j.temporalgraph.benchmarks.BenchmarkUtils;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.entitystores.RelationshipStore;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Benchmark)
public class RelationshipStoreGetRelationshipsBenchmark {

    @Param({"100000", "1000000", "10000000"})
    private int operations;

    @Param({"persistentlinkedlist", "persistentdoublelist"})
    private String type;

    @Param({"uniform", "zipf"})
    private String distribution;

    RelationshipStore store;
    private static final int REL_TYPE = 42;
    List<InMemoryRelationship> relationshipList;
    private String debug = null;
    private Random random;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
        store = BenchmarkUtils.relationshipStoreOf(type);
        relationshipList = (distribution.equals("zipf"))
                ? BenchmarkUtils.createRelationshipsZipf(operations, REL_TYPE)
                : BenchmarkUtils.createRelationships(operations, REL_TYPE);
        store.addRelationships(relationshipList);
        random = new Random(42L);
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws IOException {
        if (debug != null) {
            System.out.println(debug);
        }
        store.shutdown();
        store = null;
        relationshipList = null;
    }

    @Benchmark
    public void testGetNodeRelationships(Blackhole blackhole) throws IOException {
        var nodeId = BenchmarkUtils.randInt(random, 0, operations - 1);
        var timestamp = BenchmarkUtils.randInt(random, 0, operations - 1);
        var rels = store.getRelationships(nodeId, RelationshipDirection.BOTH, timestamp);
        blackhole.consume(rels.size());
    }

    @Benchmark
    public void testGetAllRelationships(Blackhole blackhole) throws IOException {
        var timestamp = BenchmarkUtils.randInt(random, 0, operations - 1);
        var rels = store.getAllRelationships(timestamp);
        blackhole.consume(rels.size());
    }

    @Benchmark
    public void testGetRelationship(Blackhole blackhole) throws IOException {
        var nodeId = BenchmarkUtils.randInt(random, 0, operations - 1);
        var timestamp = BenchmarkUtils.randInt(random, 0, operations - 1);
        var rel = store.getRelationship(nodeId, timestamp);
        blackhole.consume(rel);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RelationshipStoreGetRelationshipsBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
