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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.temporalgraph.TestUtils.checkAllNodesExist;
import static org.neo4j.temporalgraph.TestUtils.checkAllRelationshipsExist;
import static org.neo4j.temporalgraph.TestUtils.checkNodeExists;
import static org.neo4j.temporalgraph.TestUtils.checkRelationshipExists;
import static org.neo4j.temporalgraph.TestUtils.compareGraphsWithoutRelationshipPointers;
import static org.neo4j.temporalgraph.TestUtils.getPageCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.entities.TemporalGraph;
import org.neo4j.temporalgraph.timeindex.SnapshotCreationPolicy;
import org.neo4j.temporalgraph.timeindex.timestore.disk.PersistentTimeStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith({TestDirectorySupportExtension.class})
class PersistentTimeStoreTests {
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
    void shouldReturnCorrectNodes() throws IOException {
        // Create a store and add nodes
        var policy = new SnapshotCreationPolicy(5);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        // Add plain nodes to the store (timestamps 0 - 4)
        List<InMemoryNode> nodes = new ArrayList<>();
        for (long i = 0; i < 5; ++i) {
            nodes.add(new InMemoryNode(i, i));
        }

        for (var n : nodes) {
            store.addUpdate(n);
        }
        store.takeSnapshot();

        // Add a label and a property to all nodes (timestamps 5 - 9)
        List<InMemoryNode> nodesWithLabelAndProperty = new ArrayList<>();
        for (long i = 0; i < 5; ++i) {
            var node = new InMemoryNode(i, i + 5);
            node.addLabel("Node");
            node.addProperty("id", i);
            nodesWithLabelAndProperty.add(node);
        }

        for (var n : nodesWithLabelAndProperty) {
            store.addUpdate(n);
        }
        store.takeSnapshot();

        // Check individual nodes at different timestamps
        for (var n : nodes) {
            checkNodeExists(store, n, n.getStartTimestamp());
        }
        for (var n : nodesWithLabelAndProperty) {
            checkNodeExists(store, n, n.getStartTimestamp());
        }

        // Check that both versions of the nodes exist in the store
        checkAllNodesExist(store, nodes, 4L);
        checkAllNodesExist(store, nodesWithLabelAndProperty, 9L);
    }

    @Test
    void shouldReturnCorrectRelationships() throws IOException {
        // Create a store and add nodes
        var policy = new SnapshotCreationPolicy(5);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        // Add plain nodes to the store (timestamps 0 - 4)
        List<InMemoryNode> nodes = new ArrayList<>();
        for (long i = 0; i < 5; ++i) {
            nodes.add(new InMemoryNode(i, i));
        }

        for (var n : nodes) {
            store.addUpdate(n);
        }
        store.takeSnapshot();

        // Add relationships (timestamps 5 - 9)
        List<InMemoryRelationship> relsWithLabelAndProperty = new ArrayList<>();
        for (long i = 0; i < 5; ++i) {
            var start = i;
            var end = (start == 4) ? 0 : start;
            var rel = new InMemoryRelationship(i, start, end, relType, i + 5);
            rel.addProperty("id", i);
            relsWithLabelAndProperty.add(rel);
        }
        for (var r : relsWithLabelAndProperty) {
            store.addUpdate(r);
        }
        store.takeSnapshot();

        // Delete rels 0 and 1 (timestamps 10 - 11)
        var rel0 = new InMemoryRelationship(0L, 0L, 1L, relType, 10L);
        rel0.setDeleted();
        store.addUpdate(rel0);
        var rel1 = new InMemoryRelationship(1L, 1L, 2L, relType, 11L);
        rel1.setDeleted();
        store.addUpdate(rel1);

        store.flushLog();

        List<InMemoryRelationship> newRelsWithLabelAndProperty = new ArrayList<>();
        newRelsWithLabelAndProperty.add(relsWithLabelAndProperty.get(2));
        newRelsWithLabelAndProperty.add(relsWithLabelAndProperty.get(3));
        newRelsWithLabelAndProperty.add(relsWithLabelAndProperty.get(4));

        // Check individual nodes at different timestamps
        for (var r : relsWithLabelAndProperty) {
            checkRelationshipExists(store, r, r.getStartTimestamp());
        }

        // Check that both versions of the nodes exist in the store
        checkAllRelationshipsExist(store, relsWithLabelAndProperty, 9L);
        checkAllRelationshipsExist(store, newRelsWithLabelAndProperty, 11L);
    }

    @Test
    void shouldExpandNodeForMultipleHopsDirected() throws IOException {
        // Create a store and add nodes
        var policy = new SnapshotCreationPolicy(5);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        var nodes = new ArrayList<>(
                Arrays.asList(new InMemoryNode(0L, 0), new InMemoryNode(1L, 0), new InMemoryNode(2L, 0)));
        for (var n : nodes) {
            store.addUpdate(n);
        }
        var rels = new ArrayList<>(Arrays.asList(
                new InMemoryRelationship(
                        0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, 0),
                new InMemoryRelationship(
                        1L, nodes.get(2).getEntityId(), nodes.get(1).getEntityId(), relType, 0)));
        for (var r : rels) {
            store.addUpdate(r);
        }

        var newRel1 = new InMemoryRelationship(
                2L, nodes.get(0).getEntityId(), nodes.get(2).getEntityId(), relType, 0);
        store.addUpdate(newRel1);

        // Add a new node 3 and relationship between node 2 and node 3
        var timestamp1 = 16L;
        var newNode1 = new InMemoryNode(3L, timestamp1);
        store.addUpdate(newNode1);
        var newRel2 = new InMemoryRelationship(
                3L, nodes.get(2).getEntityId(), newNode1.getEntityId(), relType, timestamp1, false, false);
        store.addUpdate(newRel2);

        // Add a new node 4 and relationship between node 3 and node 4
        var timestamp2 = 42L;
        var newNode2 = new InMemoryNode(4L, timestamp2);
        store.addUpdate(newNode2);
        var newRel3 = new InMemoryRelationship(
                4L, newNode1.getEntityId(), newNode2.getEntityId(), relType, timestamp2, false, false);
        store.addUpdate(newRel3);

        store.flushLog();

        // Expand node 0 at time 0 for three hops: i.e., node 1 (twice) and node 2
        var neighbours = store.expand(0L, RelationshipDirection.OUTGOING, 3, 0L);
        assertEquals(nodes.get(1), neighbours.get(0)); // node 1
        assertEquals(nodes.get(2), neighbours.get(1)); // node 2
        assertEquals(nodes.get(1), neighbours.get(2)); // node 1

        // Expand node 0 at time 16 for three hop: node 1 (twice), node 2, and node 3
        neighbours = store.expand(0L, RelationshipDirection.OUTGOING, 3, timestamp1);
        assertEquals(nodes.get(1), neighbours.get(0)); // node 1
        assertEquals(nodes.get(2), neighbours.get(1)); // node 2
        assertEquals(nodes.get(1), neighbours.get(2)); // node 1
        assertEquals(newNode1, neighbours.get(3)); // node 3

        // Check again all neighbours for node 0 at time 42: node 1, node 2, node 3, and node 4
        neighbours = store.expand(0L, RelationshipDirection.OUTGOING, 3, timestamp2);
        assertEquals(nodes.get(1), neighbours.get(0)); // node 1
        assertEquals(nodes.get(2), neighbours.get(1)); // node 2
        assertEquals(nodes.get(1), neighbours.get(2)); // node 1
        assertEquals(newNode1, neighbours.get(3)); // node 3
        assertEquals(newNode2, neighbours.get(4)); // node 4
    }

    @Test
    void shouldReturnRelationshipsBasedOnTime() throws IOException {
        // Create a store with 100_000 nodes.
        // Then, add 100_000 relationships twice, and finally delete 50_000.
        // It should return all the visible relationships according to the given timestamp.
        var policy = new SnapshotCreationPolicy(10_000);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        final int batchSize = 100_000;

        // Add nodes at timestamp1
        var timestamp1 = 0L;
        List<InMemoryNode> nodesAtTimestamp1 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            nodesAtTimestamp1.add(new InMemoryNode(i, timestamp1));
        }

        for (int i = 0; i < batchSize; ++i) {
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
        }

        for (int i = 0; i < batchSize; ++i) {
            store.addUpdate(relsAtTimestamp2.get(i));
        }
        store.takeSnapshot();

        // Add relationships at timestamp3
        var timestamp3 = 128L;
        List<InMemoryRelationship> relsAtTimestamp3 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            relsAtTimestamp3.add(new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp3));
        }

        for (int i = 0; i < batchSize; ++i) {
            store.addUpdate(relsAtTimestamp3.get(i));
        }
        store.takeSnapshot();

        // Remove relationships from timestamp2
        var timestamp4 = 256L;
        List<InMemoryRelationship> relsAtTimestamp4 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            relsAtTimestamp4.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp4, true, false));
        }

        for (int i = 0; i < batchSize; ++i) {
            store.addUpdate(relsAtTimestamp4.get(i));
        }
        store.takeSnapshot();

        // Check rels at timestamp1
        var resultRels1 = store.getGraph(timestamp1).getRelationships();
        assertEquals(0, resultRels1.size());

        // Check nodes at timestamp2
        var resultRels2 = store.getGraph(timestamp2).getRelationships();
        assertEquals(batchSize, resultRels2.size());
        resultRels2.sort(Comparator.comparing(InMemoryRelationship::getEntityId));
        // The first 100_000 rels
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    relsAtTimestamp2.get(i).getEntityId(), resultRels2.get(i).getEntityId());
            assertEquals(
                    relsAtTimestamp2.get(i).getStartNode(), resultRels2.get(i).getStartNode());
            assertEquals(
                    relsAtTimestamp2.get(i).getEndNode(), resultRels2.get(i).getEndNode());
        }

        // Check nodes at timestamp3
        var resultRels3 = store.getGraph(timestamp3).getRelationships();
        assertEquals(2 * batchSize, resultRels3.size());
        resultRels3.sort(Comparator.comparing(InMemoryRelationship::getEntityId));
        // The first 100_000 rels
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    relsAtTimestamp2.get(i).getEntityId(), resultRels3.get(i).getEntityId());
            assertEquals(
                    relsAtTimestamp2.get(i).getStartNode(), resultRels3.get(i).getStartNode());
            assertEquals(
                    relsAtTimestamp2.get(i).getEndNode(), resultRels3.get(i).getEndNode());
        }
        // The second 100_000 rels
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    relsAtTimestamp3.get(i).getEntityId(),
                    resultRels3.get(i + batchSize).getEntityId());
            assertEquals(
                    relsAtTimestamp3.get(i).getStartNode(),
                    resultRels3.get(i + batchSize).getStartNode());
            assertEquals(
                    relsAtTimestamp3.get(i).getEndNode(),
                    resultRels3.get(i + batchSize).getEndNode());
        }

        // Check nodes at timestamp4
        var resultRels4 = store.getGraph(timestamp4).getRelationships();
        assertEquals(batchSize, resultRels4.size());
        resultRels4.sort(Comparator.comparing(InMemoryRelationship::getEntityId));
        // The remaining 100_000 rels
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    relsAtTimestamp3.get(i).getEntityId(), resultRels4.get(i).getEntityId());
            assertEquals(
                    relsAtTimestamp3.get(i).getStartNode(), resultRels4.get(i).getStartNode());
            assertEquals(
                    relsAtTimestamp3.get(i).getEndNode(), resultRels4.get(i).getEndNode());
        }
    }

    @Test
    void shouldReturnNodeHistory() throws IOException {
        var policy = new SnapshotCreationPolicy(5);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        // Add a node
        var node1 = new InMemoryNode(0L, 0L);
        store.addUpdate(node1);

        // Change its label/properties
        var node2 = new InMemoryNode(0L, 42L, false, true);
        node2.addLabel("Node");
        node2.addProperty("id", 1);
        store.addUpdate(node2);

        // Delete the node
        var node3 = new InMemoryNode(0L, 128L);
        node3.setDeleted();
        store.addUpdate(node3);

        store.flushLog();

        // Get the node history
        var nodeList = store.getNode(0L, 0L, 128L);
        assertEquals(3, nodeList.size());
        assertEquals(node1, nodeList.get(0));
        assertEquals(node2, nodeList.get(1));
        assertTrue(nodeList.get(2).isDeleted());
    }

    @Test
    void shouldReturnRelationshipHistory() throws IOException {
        var policy = new SnapshotCreationPolicy(5);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        // Add a relationship
        var rel1 = new InMemoryRelationship(0L, 2L, 3L, relType, 0L);
        store.addUpdate(rel1);

        // Change its properties
        var rel2 = new InMemoryRelationship(0L, 2L, 3L, relType, 42L, false, true);
        rel2.addProperty("id", 1);
        store.addUpdate(rel2);

        // Delete the relationship
        var rel3 = new InMemoryRelationship(0L, 2L, 3L, relType, 128L, true, false);
        store.addUpdate(rel3);

        store.flushLog();

        // Get the relationship history
        var relList = store.getRelationship(0L, 0L, 128L);
        assertEquals(3, relList.size());
        assertEquals(rel1, relList.get(0));
        assertEquals(rel2, relList.get(1));
        assertTrue(relList.get(2).isDeleted());
    }

    @Test
    void shouldReturnNeighbourhoodHistory() throws IOException {
        var policy = new SnapshotCreationPolicy(5);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        // Add a node
        var node1 = new InMemoryNode(0L, 0L);
        store.addUpdate(node1);

        // Add relationships
        var rel1 = new InMemoryRelationship(0L, 0L, 1L, relType, 42);
        rel1.addProperty("id", 1);
        store.addUpdate(rel1);

        // Add more relationships
        var rel2 = new InMemoryRelationship(1L, 0L, 2L, relType, 128L);
        rel2.addProperty("id", 2);
        var rel3 = new InMemoryRelationship(2L, 0L, 3L, relType, 128L);
        rel3.addProperty("id", 3);
        store.addUpdate(rel2);
        store.addUpdate(rel3);

        store.flushLog();

        // Get the neighbourhood history
        var neighbourhoodList = store.getRelationships(0L, RelationshipDirection.BOTH, 0L, 128L);
        assertEquals(3, neighbourhoodList.size());
        assertEquals(rel1, neighbourhoodList.get(1).get(0));
        assertTrue(rel2.equalsWithoutPointers(neighbourhoodList.get(2).get(1)));
        assertTrue(rel3.equalsWithoutPointers(neighbourhoodList.get(2).get(2)));
    }

    @Test
    void shouldReturnGraphHistory() throws IOException {
        // Create a store and add nodes
        var policy = new SnapshotCreationPolicy(5);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        // Add a graph
        var node1 = new InMemoryNode(0L, 0L);
        var node2 = new InMemoryNode(1L, 0L);
        store.addUpdate(node1);
        store.addUpdate(node2);
        var rel1 = new InMemoryRelationship(0L, 0L, 1L, relType, 0);
        rel1.addProperty("id", 1);
        store.addUpdate(rel1);

        var graph1 = InMemoryGraph.createGraph();
        graph1.updateNode(node1);
        graph1.updateNode(node2);
        graph1.updateRelationship(rel1);

        // Add more nodes and relationships
        var node3 = new InMemoryNode(2L, 42L);
        store.addUpdate(node3);
        var rel2 = new InMemoryRelationship(1L, 0L, 2L, relType, 42L);
        rel2.addProperty("id", 2);
        var rel3 = new InMemoryRelationship(2L, 1L, 2L, relType, 42L);
        rel3.addProperty("id", 3);
        store.addUpdate(rel2);
        store.addUpdate(rel3);

        store.flushLog();

        var graph2 = InMemoryGraph.createGraph();
        graph2.updateNode(node3);
        graph2.updateRelationship(rel2);
        graph2.updateRelationship(rel3);

        // Get the graph history
        var graphList = store.getGraph(0L, 42L, 42L);
        assertEquals(2, graphList.size());
        compareGraphsWithoutRelationshipPointers(graph1, graphList.get(0));
        compareGraphsWithoutRelationshipPointers(graph2, graphList.get(1));
    }

    @Test
    void shouldReturnGraphWindow() throws IOException {
        // Create a store and add nodes
        var policy = new SnapshotCreationPolicy(5);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        // Add nodes [0, 4]
        var node1 = new InMemoryNode(0L, 0L);
        var node2 = new InMemoryNode(1L, 0L);
        var node3 = new InMemoryNode(2L, 0L);
        var node4 = new InMemoryNode(3L, 0L);
        var node5 = new InMemoryNode(4L, 0L);
        store.addUpdate(node1);
        store.addUpdate(node2);
        store.addUpdate(node3);
        store.addUpdate(node4);
        store.addUpdate(node5);

        // Add relationships 0->1, 1->2, 1->3, 2->3, 4->3
        var rel1 = new InMemoryRelationship(0L, 0L, 1L, relType, 1);
        var rel2 = new InMemoryRelationship(1L, 1L, 2L, relType, 2);
        var rel3 = new InMemoryRelationship(2L, 1L, 3L, relType, 3);
        var rel4 = new InMemoryRelationship(3L, 2L, 3L, relType, 1);
        var rel5 = new InMemoryRelationship(4L, 4L, 3L, relType, 1);
        store.addUpdate(rel1);
        store.addUpdate(rel2);
        store.addUpdate(rel3);
        store.addUpdate(rel4);
        store.addUpdate(rel5);

        // Flush data to disk
        store.flushLog();

        // Result contains nodes 1, 2, 3 and rels 1->2, 1->3, 2->3
        var resultGraph = InMemoryGraph.createGraph();
        resultGraph.updateNode(node2);
        resultGraph.updateNode(node3);
        resultGraph.updateNode(node4);
        resultGraph.updateRelationship(rel2);
        resultGraph.updateRelationship(rel3);
        resultGraph.updateRelationship(rel4);

        // Get the graph window
        var window = store.getWindow(2, 3);
        assertEquals(resultGraph, window);
    }

    @Test
    void shouldReturnTemporalGraph() throws IOException {
        // Create a store and add nodes
        var policy = new SnapshotCreationPolicy(5);
        var store = new PersistentTimeStore(
                policy,
                getPageCache(directory),
                directory.getFileSystem(),
                directory.homePath().resolve(TIMESTORE_LOG),
                directory.homePath().resolve(TIMESTORE_INDEX),
                namesToIds,
                idsToNames);

        // Add nodes [0, 3]
        var node1 = new InMemoryNode(0L, 0L);
        var node2 = new InMemoryNode(1L, 0L);
        var node3 = new InMemoryNode(2L, 0L);
        var node4 = new InMemoryNode(3L, 0L);
        store.addUpdate(node1);
        store.addUpdate(node2);
        store.addUpdate(node3);
        store.addUpdate(node4);

        // Add relationships 0->1, 1->2, 1->3, 2->3
        var rel1 = new InMemoryRelationship(0L, 0L, 1L, relType, 0);
        var rel2 = new InMemoryRelationship(1L, 1L, 2L, relType, 0);
        var rel3 = new InMemoryRelationship(2L, 1L, 3L, relType, 0);
        var rel4 = new InMemoryRelationship(3L, 2L, 3L, relType, 0);
        store.addUpdate(rel1);
        store.addUpdate(rel2);
        store.addUpdate(rel3);
        store.addUpdate(rel4);

        // Update node 0 and relationship 1->3
        var node1_ = new InMemoryNode(0L, 2L, false, true);
        node1_.addProperty("id", 42L);
        var rel3_ = new InMemoryRelationship(2L, 1, 3L, relType, 2L, false, true);
        rel3_.addProperty("id", 128L);
        store.addUpdate(node1_);
        store.addUpdate(rel3_);

        // Delete relationship 2->3
        var rel4_ = new InMemoryRelationship(3L, 2L, 3L, relType, 2L, true, false);
        store.addUpdate(rel4_);

        // Flush data to disk
        store.flushLog();

        // Result contains the initial nodes and relationships
        var resultGraph = new TemporalGraph();
        resultGraph.addNode(node1);
        resultGraph.addNode(node2);
        resultGraph.addNode(node3);
        resultGraph.addNode(node4);
        resultGraph.addRelationship(rel1);
        resultGraph.addRelationship(rel2);
        resultGraph.addRelationship(rel3);
        resultGraph.addRelationship(rel4);
        // along with the duplicated entities
        var newNode1 = new InMemoryNode(0L, 2L, false, true);
        newNode1.addProperty("id", 42L);
        resultGraph.addNode(newNode1);
        var newRel3 = new InMemoryRelationship(2L, 1L, 3L, relType, 2L, false, true);
        newRel3.addProperty("id", 128L);
        resultGraph.addRelationship(newRel3);
        resultGraph.deleteRelationship((int) rel4_.getEntityId(), rel4_.getStartTimestamp());
        resultGraph.setEndTime(3);

        // Get the temporal graph
        var temporalGraph = store.getTemporalGraph(1L, 3L);
        assertEquals(resultGraph, temporalGraph);
    }
}
