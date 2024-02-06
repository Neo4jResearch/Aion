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
import static org.neo4j.temporalgraph.TestUtils.getPageCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.temporalgraph.algorithms.AvgAggregator;
import org.neo4j.temporalgraph.algorithms.DynamicBFS;
import org.neo4j.temporalgraph.algorithms.DynamicCommunityDetection;
import org.neo4j.temporalgraph.algorithms.DynamicGlobalAggregation;
import org.neo4j.temporalgraph.algorithms.DynamicNodeSimilarity;
import org.neo4j.temporalgraph.algorithms.DynamicPageRank;
import org.neo4j.temporalgraph.algorithms.DynamicPageRankV2;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.timeindex.SnapshotCreationPolicy;
import org.neo4j.temporalgraph.timeindex.timestore.disk.PersistentTimeStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith({TestDirectorySupportExtension.class})
class DynamicAlgorithmsTests {
    @Inject
    private TestDirectory directory;

    private static final String TIMESTORE_LOG = "tslog";
    private static final String TIMESTORE_INDEX = "tsindex";
    private static final int relType = 42;
    private static final Map<String, Integer> namesToIds = new HashMap<>();
    private static final Map<Integer, String> idsToNames = new HashMap<>();

    @BeforeAll
    static void setup() {
        namesToIds.put("Node", 0);
        namesToIds.put("id", 1);
        idsToNames.put(0, "Node");
        idsToNames.put(1, "id");
    }

    @AfterAll
    static void tearDown() {
        namesToIds.clear();
        idsToNames.clear();
    }

    @Test
    void shouldRunDynamicPageRank() throws IOException {
        var policy = new SnapshotCreationPolicy(200);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        final int batchSize = 100;

        // Add nodes at timestamp1
        var timestamp1 = 0L;
        List<InMemoryNode> nodesAtTimestamp1 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            nodesAtTimestamp1.add(new InMemoryNode(i, timestamp1));
            store.addUpdate(nodesAtTimestamp1.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp2
        var timestamp2 = 42L;
        List<InMemoryRelationship> relsAtTimestamp2 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            relsAtTimestamp2.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp2));
            store.addUpdate(relsAtTimestamp2.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp3
        var timestamp3 = 128L;
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            var rel = new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp3);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Remove relationships from timestamp2
        var timestamp4 = 256L;
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            var rel = new InMemoryRelationship(i, startNode, endNode, relType, timestamp4, true, false);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Remove half nodes from timestamp1
        var timestamp5 = 512L;
        for (int i = 0; i < batchSize / 2; ++i) {
            var node = new InMemoryNode(nodesAtTimestamp1.get(i).getEntityId(), timestamp5, true, false);
            store.addUpdate(node);
        }
        store.takeSnapshot();

        // Initialize PageRank with the graph at timestamp2
        var pagerank = new DynamicPageRank();
        pagerank.initialize(store.getGraph(timestamp2));
        var results1 = pagerank.getResult();
        assertEquals(batchSize, results1.size());

        // Compute PageRank for timestamp3
        var diff1 = store.getDiff(timestamp2 + 1, timestamp3);
        pagerank.update(diff1);
        var results2 = pagerank.getResult();
        assertEquals(batchSize, results2.size());

        // Compute PageRank for timestamp4
        var diff2 = store.getDiff(timestamp3 + 1, timestamp4);
        pagerank.update(diff2);
        var results3 = pagerank.getResult();
        assertEquals(batchSize, results3.size());

        // Compute PageRank for timestamp5
        var diff3 = store.getDiff(timestamp4 + 1, timestamp5);
        pagerank.update(diff3);
        var results4 = pagerank.getResult();
        assertEquals(batchSize / 2, results4.size());
    }

    @Test
    void shouldRunDynamicPageRankV2() throws IOException {
        var policy = new SnapshotCreationPolicy(200);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        final int batchSize = 100;

        // Add nodes at timestamp1
        var timestamp1 = 0L;
        List<InMemoryNode> nodesAtTimestamp1 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            nodesAtTimestamp1.add(new InMemoryNode(i, timestamp1));
            store.addUpdate(nodesAtTimestamp1.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp2
        var timestamp2 = 42L;
        List<InMemoryRelationship> relsAtTimestamp2 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            relsAtTimestamp2.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp2));
            store.addUpdate(relsAtTimestamp2.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp3
        var timestamp3 = 128L;
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            var rel = new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp3);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Remove relationships from timestamp2
        var timestamp4 = 256L;
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            var rel = new InMemoryRelationship(i, startNode, endNode, relType, timestamp4, true, false);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Remove half nodes from timestamp1
        var timestamp5 = 512L;
        for (int i = 0; i < batchSize / 2; ++i) {
            var node = new InMemoryNode(nodesAtTimestamp1.get(i).getEntityId(), timestamp5, true, false);
            store.addUpdate(node);
        }
        store.takeSnapshot();

        // Initialize PageRank with the graph at timestamp2
        var pagerank = new DynamicPageRankV2();
        pagerank.initialize(store.getGraph(timestamp2));
        var results1 = pagerank.getResult();
        assertEquals(batchSize, results1.size());

        // Compute PageRank for timestamp3
        var diff1 = store.getDiff(timestamp2 + 1, timestamp3);
        pagerank.update(diff1);
        var results2 = pagerank.getResult();
        assertEquals(batchSize, results2.size());

        // Compute PageRank for timestamp4
        var diff2 = store.getDiff(timestamp3 + 1, timestamp4);
        pagerank.update(diff2);
        var results3 = pagerank.getResult();
        assertEquals(batchSize, results3.size());

        // Compute PageRank for timestamp5
        var diff3 = store.getDiff(timestamp4 + 1, timestamp5);
        pagerank.update(diff3);
        var results4 = pagerank.getResult();
        assertEquals(batchSize / 2, results4.size());
    }

    @Test
    void shouldRunDynamicGlobalAggregation() throws IOException {
        var policy = new SnapshotCreationPolicy(200);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        final int batchSize = 100;

        // Add nodes at timestamp1
        var timestamp1 = 0L;
        for (int i = 0; i < batchSize; ++i) {
            var node = new InMemoryNode(i, timestamp1);
            node.addProperty("id", i);
            store.addUpdate(node);
        }
        store.takeSnapshot();

        // Add new nodes at timestamp2
        var timestamp2 = 42L;
        for (int i = 0; i < batchSize; ++i) {
            var id = i + batchSize;
            var node = new InMemoryNode(id, timestamp2);
            node.addProperty("id", id);
            store.addUpdate(node);
        }
        store.takeSnapshot();

        // Add new nodes at timestamp3
        var timestamp3 = 128L;
        for (int i = 0; i < batchSize; ++i) {
            var id = i + 2 * batchSize;
            var node = new InMemoryNode(id, timestamp3);
            node.addProperty("id", id);
            store.addUpdate(node);
        }
        store.takeSnapshot();

        // Initialize global aggregation with the graph at timestamp1
        var globalAggr = new DynamicGlobalAggregation(new AvgAggregator(), "id");
        globalAggr.initialize(store.getGraph(timestamp1));
        var result1 = globalAggr.getResult();
        assertEquals(49.5, result1, 0.001);

        // Compute global aggregation for timestamp2
        var diff1 = store.getDiff(timestamp1 + 1, timestamp2);
        globalAggr.update(diff1);
        var result2 = globalAggr.getResult();
        assertEquals(99.5, result2, 0.001);

        // Compute PageRank for timestamp3
        var diff2 = store.getDiff(timestamp2 + 1, timestamp3);
        globalAggr.update(diff2);
        var result3 = globalAggr.getResult();
        assertEquals(149.5, result3, 0.001);
    }

    @Test
    void shouldRunDynamicCommunityDetection() throws IOException {
        var policy = new SnapshotCreationPolicy(200);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        final int batchSize = 100;

        // Add nodes at timestamp1
        var timestamp1 = 0L;
        List<InMemoryNode> nodesAtTimestamp1 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            nodesAtTimestamp1.add(new InMemoryNode(i, timestamp1));
            store.addUpdate(nodesAtTimestamp1.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp2
        var timestamp2 = 42L;
        List<InMemoryRelationship> relsAtTimestamp2 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            relsAtTimestamp2.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp2));
            store.addUpdate(relsAtTimestamp2.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp3
        var timestamp3 = 128L;
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            var rel = new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp3);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Remove relationships from timestamp2
        var timestamp4 = 256L;
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            var rel = new InMemoryRelationship(i, startNode, endNode, relType, timestamp4, true, false);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Remove half nodes from timestamp1
        var timestamp5 = 512L;
        for (int i = 0; i < batchSize / 2; ++i) {
            var node = new InMemoryNode(nodesAtTimestamp1.get(i).getEntityId(), timestamp5, true, false);
            store.addUpdate(node);
        }
        store.takeSnapshot();

        // Initialize CommunityDetection with the graph at timestamp2
        var labelRank = new DynamicCommunityDetection();
        labelRank.initialize(store.getGraph(timestamp2));
        var results1 = labelRank.getResult();
        assertEquals(batchSize, results1.size());

        // Compute communities for timestamp3
        var diff1 = store.getDiff(timestamp2 + 1, timestamp3);
        labelRank.update(diff1);
        var results2 = labelRank.getResult();
        assertEquals(batchSize, results2.size());

        // Compute communities for timestamp4
        var diff2 = store.getDiff(timestamp3 + 1, timestamp4);
        labelRank.update(diff2);
        var results3 = labelRank.getResult();
        assertEquals(batchSize, results3.size());

        // Compute communities for timestamp5
        var diff3 = store.getDiff(timestamp4 + 1, timestamp5);
        labelRank.update(diff3);
        var results4 = labelRank.getResult();
        assertEquals(batchSize / 2, results4.size());
    }

    @Test
    void shouldRunDynamicBFS() throws IOException {
        var policy = new SnapshotCreationPolicy(200);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        final int batchSize = 100;

        // Add nodes at timestamp1
        var timestamp1 = 0L;
        List<InMemoryNode> nodesAtTimestamp1 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            nodesAtTimestamp1.add(new InMemoryNode(i, timestamp1));
            store.addUpdate(nodesAtTimestamp1.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp2
        var timestamp2 = 42L;
        List<InMemoryRelationship> relsAtTimestamp2 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            relsAtTimestamp2.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp2));
            store.addUpdate(relsAtTimestamp2.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp3
        var timestamp3 = 128L;
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            var rel = new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp3);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Remove relationships from timestamp2
        var timestamp4 = 256L;
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            var rel = new InMemoryRelationship(i, startNode, endNode, relType, timestamp4, true, false);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Initialize BFS with the graph at timestamp2
        var bfs = new DynamicBFS(0);
        bfs.initialize(store.getGraph(timestamp2));
        var results1 = bfs.getResult();
        assertEquals(batchSize, results1.size());

        // Compute BFS for timestamp3
        var diff1 = store.getDiff(timestamp2 + 1, timestamp3);
        bfs.update(diff1);
        var results2 = bfs.getResult();
        assertEquals(batchSize, results2.size());

        // Compute BFS for timestamp4
        var diff2 = store.getDiff(timestamp3 + 1, timestamp4);
        bfs.update(diff2);
        var results3 = bfs.getResult();
        assertEquals(batchSize, results3.size());
    }

    @Test
    void shouldRunDynamicNodeSimilarity() throws IOException {
        var policy = new SnapshotCreationPolicy(200);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        final int batchSize = 100;

        // Add nodes at timestamp1
        var timestamp1 = 0L;
        List<InMemoryNode> nodesAtTimestamp1 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            nodesAtTimestamp1.add(new InMemoryNode(i, timestamp1));
            store.addUpdate(nodesAtTimestamp1.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp2 from nodes [0, 50) to [50, 100)
        var timestamp2 = 42L;
        List<InMemoryRelationship> relsAtTimestamp2 = new ArrayList<>();
        for (int i = 0; i < batchSize / 2; ++i) {
            var startNode = i;
            var endNode = 50 + i;
            relsAtTimestamp2.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp2));
            store.addUpdate(relsAtTimestamp2.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp3 from nodes [0, 50) to [50, 100)
        var timestamp3 = 128L;
        for (int i = 0; i < batchSize / 2; ++i) {
            var startNode = i;
            var endNode = (i + 51 == batchSize) ? 50 : 51 + i;
            var rel = new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp3);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Remove half relationships from timestamp2
        var timestamp4 = 256L;
        for (int i = 0; i < batchSize / 4; ++i) {
            var startNode = i;
            var endNode = 50 + i;
            var rel = new InMemoryRelationship(i, startNode, endNode, relType, timestamp4, true, false);
            store.addUpdate(rel);
        }
        store.takeSnapshot();

        // Initialize NodeSimilarity with the graph at timestamp2
        var nodeSimilarity = new DynamicNodeSimilarity(false);
        nodeSimilarity.initialize(store.getGraph(timestamp2));
        var results1 = nodeSimilarity.getResult();
        assertEquals(0, results1.size());

        // Compute NodeSimilarity for timestamp3
        var diff1 = store.getDiff(timestamp2 + 1, timestamp3);
        nodeSimilarity.update(diff1);
        var results2 = nodeSimilarity.getResult();
        assertEquals(batchSize, results2.size());

        // Compute NodeSimilarity for timestamp4
        var diff2 = store.getDiff(timestamp3 + 1, timestamp4);
        nodeSimilarity.update(diff2);
        var results3 = nodeSimilarity.getResult();
        assertEquals(batchSize / 2, results3.size());
    }
}
