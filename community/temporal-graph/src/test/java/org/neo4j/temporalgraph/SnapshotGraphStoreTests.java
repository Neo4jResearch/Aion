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
package org.neo4j.temporalgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.temporalgraph.TestUtils.getPageCache;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.timeindex.timestore.disk.SnapshotGraphStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith({TestDirectorySupportExtension.class})
class SnapshotGraphStoreTests {
    @Inject
    private TestDirectory directory;

    private static final String SNAPSHOT_INDEX = "sindex";

    private static final int relType = 42;

    @Test
    void shouldRetrieveGraphFromMemory() throws IOException {
        // Create a store, add graph snapshots and retrieve them
        var store = new SnapshotGraphStore(
                mebiBytes(128),
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(SNAPSHOT_INDEX),
                null,
                null);

        final int batchSize = 100_000;

        // Add the first version of the graph
        var timestamp1 = 0L;
        var graph1 = InMemoryGraph.createGraph();
        for (int i = 0; i < batchSize; ++i) {
            graph1.updateNode(new InMemoryNode(i, timestamp1));
        }
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            graph1.updateRelationship(new InMemoryRelationship(i, startNode, endNode, relType, timestamp1));
        }
        store.storeSnapshot(graph1, timestamp1);

        // Add more relationships
        var timestamp2 = 42L;
        var graph2 = graph1.copy();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            graph2.updateRelationship(new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp2));
        }
        store.storeSnapshot(graph2, timestamp2);

        var snapshot1 = store.getSnapshot(0L).getRight().copy();
        assertEquals(graph1, snapshot1);

        var snapshot2 = store.getSnapshot(33L).getRight().copy();
        assertEquals(graph1, snapshot2);

        var snapshot3 = store.getSnapshot(42L).getRight().copy();
        assertEquals(graph2, snapshot3);
    }

    @Test
    void shouldRetrieveGraphFromMemory2() throws IOException {
        // Create a store, add graph snapshots and retrieve them
        var store = new SnapshotGraphStore(
                mebiBytes(128),
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(SNAPSHOT_INDEX),
                null,
                null);

        final int batchSize = 100_000;

        // Add the first version of the graph
        var timestamp1 = 0L;
        var graph1 = InMemoryGraph.createGraph();
        for (int i = 0; i < batchSize; ++i) {
            graph1.updateNode(new InMemoryNode(i, timestamp1));
        }
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            graph1.updateRelationship(new InMemoryRelationship(i, startNode, endNode, relType, timestamp1));
        }
        store.storeSnapshot(graph1, timestamp1);

        // Add more relationships
        var timestamp2 = 42L;
        var graph2 = graph1.parallelCopy();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            graph2.updateRelationship(new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp2));
        }
        store.storeSnapshot(graph2, timestamp2);

        var snapshot1 = store.getSnapshot(0L).getRight().parallelCopy();
        assertEquals(graph1, snapshot1);

        var snapshot2 = store.getSnapshot(33L).getRight().parallelCopy();
        assertEquals(graph1, snapshot2);

        var snapshot3 = store.getSnapshot(42L).getRight().parallelCopy();
        assertEquals(graph2, snapshot3);
    }

    @Test
    void shouldRetrieveGraphFromDisk() throws IOException {
        // Create a store, add graph snapshots and retrieve them
        var store = new SnapshotGraphStore(
                0L,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(SNAPSHOT_INDEX),
                null,
                null);

        final int batchSize = 100_000;

        // Add the first version of the graph
        var timestamp1 = 0L;
        var graph1 = InMemoryGraph.createGraph();
        for (int i = 0; i < batchSize; ++i) {
            graph1.updateNode(new InMemoryNode(i, timestamp1));
        }
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            graph1.updateRelationship(new InMemoryRelationship(i, startNode, endNode, relType, timestamp1));
        }
        store.storeSnapshot(graph1, timestamp1);

        // Add more relationships
        var timestamp2 = 42L;
        var graph2 = graph1.copy();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            graph2.updateRelationship(new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp2));
        }
        store.storeSnapshot(graph2, timestamp2);

        assertEquals(graph1, store.getSnapshot(0L).getRight());
        assertEquals(graph1, store.getSnapshot(33L).getRight());
        assertEquals(graph2, store.getSnapshot(42L).getRight());
    }
}
