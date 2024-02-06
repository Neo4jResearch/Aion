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
import static org.neo4j.temporalgraph.TestUtils.checkNodeExists;
import static org.neo4j.temporalgraph.TestUtils.checkNodeNotPresent;
import static org.neo4j.temporalgraph.TestUtils.checkRelationshipExists;
import static org.neo4j.temporalgraph.TestUtils.checkRelationshipNotPresent;
import static org.neo4j.temporalgraph.TestUtils.compareGraphsWithoutRelationshipPointers;
import static org.neo4j.temporalgraph.TestUtils.label;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.InMemoryRelationshipV2;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.InMemoryLineageStore;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.values.storable.Values;

@ExtendWith({TestDirectorySupportExtension.class})
class InMemoryLineageStoreTests {

    private static List<InMemoryNode> nodes;
    private static List<InMemoryRelationship> rels;
    private static final int relType = 42;

    @BeforeAll
    static void setup() {
        var timestamp = 0L;
        // Nodes = {0, 1, 2}
        nodes = new ArrayList<>(Arrays.asList(
                new InMemoryNode(0L, timestamp), new InMemoryNode(1L, timestamp), new InMemoryNode(2L, timestamp)));

        // Rels = {0->1, 2->1}
        rels = new ArrayList<>(Arrays.asList(
                new InMemoryRelationship(
                        0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp),
                new InMemoryRelationship(
                        1L, nodes.get(2).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp)));
    }

    @AfterAll
    static void tearDown() {
        nodes.clear();
        rels.clear();
    }

    @Test
    void shouldReturnCorrectNodeAfterInsertion() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);

        // Expected stored nodes
        var node1 = new InMemoryNode(0L, 0L);
        var node2 = new InMemoryNode(1L, 0L);
        var node3 = new InMemoryNode(2L, 0L);

        // Check whether node 0 exists based on a valid timestamp and id
        checkNodeExists(store, node1, 0L);
        checkNodeExists(store, node1, 42L);
        // Check whether node 1 exists based on a valid timestamp and id
        checkNodeExists(store, node2, 0L);
        checkNodeExists(store, node2, 42L);
        // Check whether node 2 exists based on a valid timestamp and id
        checkNodeExists(store, node3, 0L);
        checkNodeExists(store, node3, 42L);

        // Check that no node is returned for invalid node id
        checkNodeNotPresent(store, 42L, 0L);

        // Check that nodes don't exist with a timestamp smaller tha 0
        checkNodeNotPresent(store, 0L, -1L);
        checkNodeNotPresent(store, 1L, -1L);
        checkNodeNotPresent(store, 2L, -1L);
    }

    @Test
    void shouldReturnCorrectNodeWhenNoDeletionPerformed() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);

        // Add modified versions of node 0
        var timestamp1 = 16L;
        var modifiedNodes1 = new ArrayList<>(List.of(new InMemoryNode(0L, timestamp1, false, true)));
        modifiedNodes1.get(0).addLabel(label.name());
        modifiedNodes1.get(0).addProperty("name", Values.utf8Value("User0"));
        store.addNodes(modifiedNodes1);

        var timestamp2 = 42L;
        var modifiedNodes2 = new ArrayList<>(List.of(new InMemoryNode(0L, timestamp2, false, true)));
        modifiedNodes2.get(0).addProperty("name", Values.utf8Value("User0-new"));
        modifiedNodes2.get(0).addProperty("age", Values.intValue(17));
        store.addNodes(modifiedNodes2);

        // Expected stored nodes
        var node1 = new InMemoryNode(0L, 0L);
        var node2 = new InMemoryNode(0L, timestamp1, false, true);
        node2.addLabel(label.name());
        node2.addProperty("name", Values.utf8Value("User0"));
        var node3 = new InMemoryNode(0L, timestamp2, false, true);
        node3.addLabel(label.name());
        node3.addProperty("name", Values.utf8Value("User0-new"));
        node3.addProperty("age", Values.intValue(17));

        // Retrieve the correct version based on the timestamp
        checkNodeExists(store, node1, 0L);
        checkNodeExists(store, node1, 15L);
        checkNodeExists(store, node2, 16L);
        checkNodeExists(store, node2, 41L);
        checkNodeExists(store, node3, 42L);
        checkNodeExists(store, node3, 100L);
    }

    @Test
    void shouldReturnCorrectNodeWithDeletions() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);

        // Add modified versions of node 0
        var timestamp1 = 16L;
        var modifiedNodes1 = new ArrayList<>(List.of(new InMemoryNode(0L, timestamp1, false, true)));
        modifiedNodes1.get(0).addLabel(label.name());
        modifiedNodes1.get(0).addProperty("name", Values.utf8Value("User0"));
        store.addNodes(modifiedNodes1);

        // Delete a property
        var timestamp2 = 42L;
        var modifiedNodes2 = new ArrayList<>(List.of(new InMemoryNode(0L, timestamp2, false, true)));
        modifiedNodes2.get(0).removeProperty("name");
        store.addNodes(modifiedNodes2);

        // Delete the node
        var timestamp3 = 128L;
        var modifiedNodes3 = new ArrayList<>(List.of(new InMemoryNode(0L, timestamp3, true, false)));
        store.addNodes(modifiedNodes3);

        // Expected stored nodes
        var node1 = new InMemoryNode(0L, 0L);
        var node2 = new InMemoryNode(0L, timestamp1, false, true);
        node2.addLabel(label.name());
        node2.addProperty("name", Values.utf8Value("User0"));
        var node3 = new InMemoryNode(0L, timestamp2, false, true);
        node3.addLabel(label.name());

        // Retrieve the correct version based on the timestamp
        checkNodeExists(store, node1, 0L);
        checkNodeExists(store, node1, 15L);
        checkNodeExists(store, node2, 16L);
        checkNodeExists(store, node2, 41L);
        checkNodeExists(store, node3, 42L);
        checkNodeExists(store, node3, 100L);
        checkNodeNotPresent(store, 0L, 128L);
        checkNodeNotPresent(store, 0L, 256L);
    }

    @Test
    void shouldReturnCorrectRelationshipAfterInsertion() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);
        store.addRelationships(rels);

        // Expected stored relationships
        var rel1 = new InMemoryRelationshipV2(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, 0);
        rel1.setSecondNextRelId(1);
        var rel2 = new InMemoryRelationshipV2(
                1L, nodes.get(2).getEntityId(), nodes.get(1).getEntityId(), relType, 0);
        rel2.setSecondPrevRelId(0);

        // Check whether node 0 exists based on a valid timestamp and id
        checkRelationshipExists(store, rel1, 0L);
        checkRelationshipExists(store, rel1, 42L);
        // Check whether node 1 exists based on a valid timestamp and id
        checkRelationshipExists(store, rel2, 0L);
        checkRelationshipExists(store, rel2, 42L);

        // Check that no node is returned for invalid node id
        checkRelationshipNotPresent(store, 42L, 0L);

        // Check that nodes don't exist with a timestamp smaller tha 0
        checkRelationshipNotPresent(store, 0L, -1L);
        checkRelationshipNotPresent(store, 1L, -1L);
    }

    @Test
    void shouldReturnCorrectRelationshipWhenNoDeletionPerformed() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);
        store.addRelationships(rels);

        // Add modified versions of rel 0
        var timestamp1 = 16L;
        var modifiedRels1 = new ArrayList<>(List.of(new InMemoryRelationship(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp1, false, true)));
        modifiedRels1.get(0).addProperty("message", Values.utf8Value("I am a relationship!"));
        store.addRelationships(modifiedRels1);

        var timestamp2 = 42L;
        var modifiedRels2 = new ArrayList<>(List.of(new InMemoryRelationship(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp2, false, true)));
        modifiedRels2.get(0).addProperty("message", Values.utf8Value("My message changed!"));
        modifiedRels2.get(0).addProperty("id", "0-1");
        store.addRelationships(modifiedRels2);

        // Expected stored relationships
        var rel1 = new InMemoryRelationshipV2(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, 0);
        rel1.setSecondNextRelId(1);

        var rel2 = new InMemoryRelationshipV2(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp1, false, true);
        rel2.addProperty("message", Values.utf8Value("I am a relationship!"));
        rel2.setSecondNextRelId(1);

        var rel3 = new InMemoryRelationshipV2(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp2, false, true);
        rel3.addProperty("message", Values.utf8Value("My message changed!"));
        rel3.addProperty("id", "0-1");
        rel3.setSecondNextRelId(1);

        // Retrieve the correct version based on the timestamp
        checkRelationshipExists(store, rel1, 0L);
        checkRelationshipExists(store, rel1, 15L);
        checkRelationshipExists(store, rel2, 16L);
        checkRelationshipExists(store, rel2, 41L);
        checkRelationshipExists(store, rel3, 42L);
        checkRelationshipExists(store, rel3, 100L);
    }

    @Test
    void shouldReturnCorrectRelationshipWithDeletions() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);
        store.addRelationships(rels);

        // Add modified versions of rel 0
        var timestamp1 = 16L;
        var modifiedRels1 = new ArrayList<>(List.of(new InMemoryRelationship(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp1, false, true)));
        modifiedRels1.get(0).addProperty("message", Values.utf8Value("I am a relationship!"));
        store.addRelationships(modifiedRels1);

        // Delete a property
        var timestamp2 = 42L;
        var modifiedRels2 = new ArrayList<>(List.of(new InMemoryRelationship(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp2, false, true)));
        modifiedRels2.get(0).removeProperty("message");
        store.addRelationships(modifiedRels2);

        // Delete the edge
        var timestamp3 = 128L;
        var modifiedRels3 = new ArrayList<>(List.of(new InMemoryRelationship(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp3, true, false)));
        store.addRelationships(modifiedRels3);

        // Expected stored relationships
        var rel1 = new InMemoryRelationshipV2(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, 0);
        rel1.setSecondNextRelId(1);

        var rel2 = new InMemoryRelationshipV2(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp1, false, true);
        rel2.addProperty("message", Values.utf8Value("I am a relationship!"));
        rel2.setSecondNextRelId(1);

        var rel3 = new InMemoryRelationshipV2(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp2, false, true);
        rel3.setSecondNextRelId(1);

        // Retrieve the correct version based on the timestamp
        checkRelationshipExists(store, rel1, 0L);
        checkRelationshipExists(store, rel1, 15L);
        checkRelationshipExists(store, rel2, 16L);
        checkRelationshipExists(store, rel2, 41L);
        checkRelationshipExists(store, rel3, 42L);
        checkRelationshipExists(store, rel3, 100L);
        checkRelationshipNotPresent(store, 0L, 128L);
        checkRelationshipNotPresent(store, 0L, 256L);
    }

    @Test
    void shouldReturnCorrectRelationshipWithDeletions2() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);
        store.addRelationships(rels);

        // Add new edge between nodes 1 and 0
        var timestamp1 = 16L;
        var newRel = new ArrayList<>(List.of(new InMemoryRelationship(
                2L, nodes.get(1).getEntityId(), nodes.get(0).getEntityId(), relType, timestamp1, false, false)));
        store.addRelationships(newRel);

        // Delete the edge in the middle of the linked list
        var timestamp2 = 42L;
        var deletedRel = new ArrayList<>(List.of(new InMemoryRelationship(
                1L, nodes.get(2).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp2, true, false)));
        store.addRelationships(deletedRel);

        // Expected stored relationships before deletion
        var rel1 = new InMemoryRelationshipV2(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, 0L);
        rel1.setSecondNextRelId(1);
        rel1.setFirstNextRelId(2);

        var rel2 = new InMemoryRelationshipV2(
                1L, nodes.get(2).getEntityId(), nodes.get(1).getEntityId(), relType, 0L);
        rel2.setSecondPrevRelId(0);
        rel2.setSecondNextRelId(2);

        var rel3 = new InMemoryRelationshipV2(
                2L, nodes.get(1).getEntityId(), nodes.get(0).getEntityId(), relType, timestamp1);
        rel3.setFirstPrevRelId(1);
        rel3.setSecondPrevRelId(0);

        // Expected stored relationships after deletion
        var newRel1 = new InMemoryRelationshipV2(
                0L, nodes.get(0).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp2, false, true);
        newRel1.setSecondNextRelId(2);
        newRel1.setFirstNextRelId(2);

        var newRel3 = new InMemoryRelationshipV2(
                2L, nodes.get(1).getEntityId(), nodes.get(0).getEntityId(), relType, timestamp2, false, true);
        newRel3.setFirstPrevRelId(0);
        newRel3.setSecondPrevRelId(0);

        // Retrieve the correct version based on the timestamp before deletion
        checkRelationshipExists(store, rel1, 16L);
        checkRelationshipExists(store, rel2, 16);
        checkRelationshipExists(store, rel3, 16L);

        // Retrieve the correct version based on the timestamp after deletion
        checkRelationshipExists(store, newRel1, 42L);
        checkRelationshipNotPresent(store, 1L, 42L);
        checkRelationshipExists(store, newRel3, 42L);
    }

    @Test
    void shouldReturnCorrectNeighbourhoodWithDeletions() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);
        store.addRelationships(rels);

        // Add new edge between nodes 1 and 0
        var timestamp1 = 16L;
        var newRel = new ArrayList<>(List.of(new InMemoryRelationship(
                2L, nodes.get(1).getEntityId(), nodes.get(0).getEntityId(), relType, timestamp1, false, false)));
        store.addRelationships(newRel);

        // Delete the edge in the middle of the linked list
        var timestamp2 = 42L;
        var deletedRel = new ArrayList<>(List.of(new InMemoryRelationship(
                1L, nodes.get(2).getEntityId(), nodes.get(1).getEntityId(), relType, timestamp2, true, false)));
        store.addRelationships(deletedRel);

        // Expected stored relationships
        var relIds1 = new long[] {1, 0};
        var relIds2 = new long[] {2, 1, 0};
        var relIds3 = new long[] {2, 0};

        // Retrieve the correct version based on the timestamp
        var i = 0;
        for (var r : store.getRelationships(1, RelationshipDirection.BOTH, 0L)) {
            assertEquals(relIds1[i++], r.getEntityId());
        }

        i = 0;
        for (var r : store.getRelationships(1, RelationshipDirection.BOTH, 16L)) {
            assertEquals(relIds2[i++], r.getEntityId());
        }

        i = 0;
        for (var r : store.getRelationships(1, RelationshipDirection.BOTH, 42L)) {
            assertEquals(relIds3[i++], r.getEntityId());
        }
    }

    @Test
    void shouldExpandNodeForOneHopDirected() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);
        store.addRelationships(rels);

        // Now, add another edge between node 0 and node 2
        var timestamp1 = 16L;
        var newRel1 = new ArrayList<>(List.of(new InMemoryRelationship(
                2L, nodes.get(0).getEntityId(), nodes.get(2).getEntityId(), relType, timestamp1, false, false)));
        store.addRelationships(newRel1);

        // Add a new node and relationship
        var timestamp2 = 42L;
        var newNode = new ArrayList<>(List.of(new InMemoryNode(3L, timestamp2)));
        store.addNodes(newNode);
        var newRel2 = new ArrayList<>(List.of(new InMemoryRelationship(
                3L, nodes.get(0).getEntityId(), newNode.get(0).getEntityId(), relType, timestamp2, false, false)));
        store.addRelationships(newRel2);

        // Get all neighbours for node 0 at time 0: i.e., node 1
        var neighbours = store.expand(0L, RelationshipDirection.OUTGOING, 1, 0L);
        assertEquals(nodes.get(1), neighbours.get(0));

        // Check again all neighbours for node 0 at time 16: node 1 and node 2
        neighbours = store.expand(0L, RelationshipDirection.OUTGOING, 1, timestamp1);
        assertEquals(nodes.get(1), neighbours.get(1));
        assertEquals(nodes.get(2), neighbours.get(0));

        // Check again all neighbours for node 0 at time 42: node 1, node 2, node 3
        neighbours = store.expand(0L, RelationshipDirection.OUTGOING, 1, timestamp2);
        assertEquals(nodes.get(1), neighbours.get(2));
        assertEquals(nodes.get(2), neighbours.get(1));
        assertEquals(newNode.get(0), neighbours.get(0));
    }

    @Test
    void shouldExpandNodeForMultipleHopsDirected() throws IOException {
        // Create a store and add nodes
        var store = new InMemoryLineageStore();
        store.addNodes(nodes);
        store.addRelationships(rels);
        var newRel1 = new ArrayList<>(List.of(new InMemoryRelationship(
                2L, nodes.get(0).getEntityId(), nodes.get(2).getEntityId(), relType, 0, false, false)));
        store.addRelationships(newRel1);

        // Add a new node 3 and relationship between node 2 and node 3
        var timestamp1 = 16L;
        var newNode1 = new ArrayList<>(List.of(new InMemoryNode(3L, timestamp1)));
        store.addNodes(newNode1);
        var newRel2 = new ArrayList<>(List.of(new InMemoryRelationship(
                3L, nodes.get(2).getEntityId(), newNode1.get(0).getEntityId(), relType, timestamp1, false, false)));
        store.addRelationships(newRel2);

        // Add a new node 4 and relationship between node 3 and node 4
        var timestamp2 = 42L;
        var newNode2 = new ArrayList<>(List.of(new InMemoryNode(4L, timestamp2)));
        store.addNodes(newNode2);
        var newRel3 = new ArrayList<>(List.of(new InMemoryRelationship(
                4L, newNode1.get(0).getEntityId(), newNode2.get(0).getEntityId(), relType, timestamp2, false, false)));
        store.addRelationships(newRel3);

        // Expand node 0 at time 0 for three hops: i.e., node 1 (twice) and node 2
        var neighbours = store.expand(0L, RelationshipDirection.OUTGOING, 3, 0L);
        assertEquals(nodes.get(2), neighbours.get(0)); // node 2
        assertEquals(nodes.get(1), neighbours.get(1)); // node 1
        assertEquals(nodes.get(1), neighbours.get(2)); // node 1

        // Expand node 0 at time 16 for three hop: node 1 (twice), node 2, and node 3
        neighbours = store.expand(0L, RelationshipDirection.OUTGOING, 3, timestamp1);
        assertEquals(nodes.get(2), neighbours.get(0)); // node 2
        assertEquals(nodes.get(1), neighbours.get(1)); // node 1
        assertEquals(newNode1.get(0), neighbours.get(2)); // node 3
        assertEquals(nodes.get(1), neighbours.get(3)); // node 1

        // Check again all neighbours for node 0 at time 42: node 1, node 2, node 3, and node 4
        neighbours = store.expand(0L, RelationshipDirection.OUTGOING, 3, timestamp2);
        assertEquals(nodes.get(2), neighbours.get(0)); // node 2
        assertEquals(nodes.get(1), neighbours.get(1)); // node 1
        assertEquals(newNode1.get(0), neighbours.get(2)); // node 3
        assertEquals(nodes.get(1), neighbours.get(3)); // node 1
        assertEquals(newNode2.get(0), neighbours.get(4)); // node 4
    }

    @Test
    void shouldReturnAllNodesBasedOnTime() throws IOException {
        // Create a store, add 100_000 nodes, then add 100_000 again, and finally delete 100_000.
        // It should return all the visible nodes according to the given timestamp.
        var store = new InMemoryLineageStore();

        final int batchSize = 100_000;

        // Add nodes at timestamp1
        var timestamp1 = 0L;
        List<InMemoryNode> nodesAtTimestamp1 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            nodesAtTimestamp1.add(new InMemoryNode(i, timestamp1));
        }
        store.addNodes(nodesAtTimestamp1);

        // Add nodes at timestamp2
        var timestamp2 = 42L;
        List<InMemoryNode> nodesAtTimestamp2 = new ArrayList<>();
        for (int i = batchSize; i < 2 * batchSize; ++i) {
            nodesAtTimestamp2.add(new InMemoryNode(i, timestamp2));
        }
        store.addNodes(nodesAtTimestamp2);

        // Delete nodes from timestamp1
        var timestamp3 = 128L;
        List<InMemoryNode> nodesAtTimestamp3 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            nodesAtTimestamp3.add(new InMemoryNode(i, timestamp3, true, false));
        }
        store.addNodes(nodesAtTimestamp3);

        // Check nodes at timestamp1
        var resultNodes1 = store.getAllNodes(timestamp1);
        assertEquals(batchSize, resultNodes1.size());
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    nodesAtTimestamp1.get(i).getEntityId(), resultNodes1.get(i).getEntityId());
        }

        // Check nodes at timestamp2
        var resultNodes2 = store.getAllNodes(timestamp2);
        assertEquals(2 * batchSize, resultNodes2.size());
        // The first 100_000 nodes
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    nodesAtTimestamp1.get(i).getEntityId(), resultNodes2.get(i).getEntityId());
        }
        // The next 100_000 nodes
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    nodesAtTimestamp2.get(i).getEntityId(),
                    resultNodes2.get(batchSize + i).getEntityId());
        }

        // Check nodes at timestamp3
        var resultNodes3 = store.getAllNodes(timestamp3);
        assertEquals(batchSize, resultNodes3.size());
        // The next 100_000 nodes
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    nodesAtTimestamp2.get(i).getEntityId(), resultNodes3.get(i).getEntityId());
        }
    }

    @Test
    void shouldReturnAllRelationshipsBasedOnTime() throws IOException {
        // Create a store with 100_000 nodes.
        // Then, add 100_000 relationships twice, and finally delete 50_000.
        // It should return all the visible relationships according to the given timestamp.
        var store = new InMemoryLineageStore();

        final int batchSize = 100_000;

        // Add nodes at timestamp1
        var timestamp1 = 0L;
        List<InMemoryNode> nodesAtTimestamp1 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            nodesAtTimestamp1.add(new InMemoryNode(i, timestamp1));
        }
        store.addNodes(nodesAtTimestamp1);

        // Add relationships at timestamp2
        var timestamp2 = 42L;
        List<InMemoryRelationship> relsAtTimestamp2 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            relsAtTimestamp2.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp2));
        }
        store.addRelationships(relsAtTimestamp2);

        // Add relationships at timestamp3
        var timestamp3 = 128L;
        List<InMemoryRelationship> relsAtTimestamp3 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            relsAtTimestamp3.add(new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp3));
        }
        store.addRelationships(relsAtTimestamp3);

        // Remove relationships from timestamp2
        var timestamp4 = 256L;
        List<InMemoryRelationship> relsAtTimestamp4 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            relsAtTimestamp4.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp4, true, false));
        }
        store.addRelationships(relsAtTimestamp4);

        // Check rels at timestamp1
        var resultRels1 = store.getAllRelationships(timestamp1);
        assertEquals(0, resultRels1.size());

        // Check nodes at timestamp2
        var resultRels2 = store.getAllRelationships(timestamp2);
        assertEquals(batchSize, resultRels2.size());
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
        var resultRels3 = store.getAllRelationships(timestamp3);
        assertEquals(2 * batchSize, resultRels3.size());
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
        var resultRels4 = store.getAllRelationships(timestamp4);
        assertEquals(batchSize, resultRels4.size());
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
        var store = new InMemoryLineageStore();

        // Add a node
        var node1 = new InMemoryNode(0L, 0L);
        store.addNodes(new ArrayList<>(List.of(node1)));

        // Change its label/properties
        var node2 = new InMemoryNode(0L, 42L, false, true);
        node2.addLabel("Node");
        node2.addProperty("id", 1);
        store.addNodes(new ArrayList<>(List.of(node2)));

        // Delete the node
        var node3 = new InMemoryNode(0L, 128L);
        node3.setDeleted();
        store.addNodes(new ArrayList<>(List.of(node3)));

        // Prepare expected results
        node1.setEndTimestamp(42L);
        node2.setEndTimestamp(128L);

        // Get the node history
        var nodeList = store.getNode(0L, 0L, 128L);
        assertEquals(2, nodeList.size());
        assertEquals(node1, nodeList.get(0));
        assertEquals(node2, nodeList.get(1));
    }

    @Test
    void shouldReturnRelationshipHistory() throws IOException {
        var store = new InMemoryLineageStore();

        // Add a relationship
        var rel1 = new InMemoryRelationship(0L, 2L, 3L, relType, 0L);
        store.addRelationships(new ArrayList<>(List.of(rel1)));

        // Change its properties
        var rel2 = new InMemoryRelationship(0L, 2L, 3L, relType, 42L, false, true);
        rel2.addProperty("id", 1);
        store.addRelationships(new ArrayList<>(List.of(rel2)));

        // Delete the relationship
        var rel3 = new InMemoryRelationship(0L, 2L, 3L, relType, 128L, true, false);
        store.addRelationships(new ArrayList<>(List.of(rel3)));

        // Prepare expected results
        rel1.setEndTimestamp(42L);
        rel2.setEndTimestamp(128L);

        // Get the relationship history
        var relList = store.getRelationship(0L, 0L, 128L);
        assertEquals(2, relList.size());
        assertTrue(relList.get(0).equalsWithoutPointers(rel1));
        assertTrue(relList.get(1).equalsWithoutPointers(rel2));
    }

    @Test
    void shouldReturnNeighbourhoodHistory() throws IOException {
        var store = new InMemoryLineageStore();

        // Add a node
        var node1 = new InMemoryNode(0L, 0L);
        store.addNodes(new ArrayList<>(List.of(node1)));

        // Add relationships
        var rel1 = new InMemoryRelationship(0L, 0L, 1L, relType, 42);
        rel1.addProperty("id", 1);
        store.addRelationships(new ArrayList<>(List.of(rel1)));

        // Add more relationships
        var rel2 = new InMemoryRelationship(1L, 0L, 2L, relType, 128L);
        rel2.addProperty("id", 2);
        var rel3 = new InMemoryRelationship(2L, 0L, 3L, relType, 128L);
        rel3.addProperty("id", 3);
        store.addRelationships(new ArrayList<>(List.of(rel2, rel3)));

        // Get the neighbourhood history
        var neighbourhoodList = store.getRelationships(0L, RelationshipDirection.BOTH, 0L, 128L);
        assertEquals(2, neighbourhoodList.size());
        assertTrue(neighbourhoodList.get(0).get(0).equalsWithoutPointers(rel1));
        assertTrue(neighbourhoodList.get(1).get(1).equalsWithoutPointers(rel2));
        assertTrue(neighbourhoodList.get(1).get(0).equalsWithoutPointers(rel3));
    }

    @Test
    void shouldReturnGraphHistory() throws IOException {
        var store = new InMemoryLineageStore();

        // Add a graph
        var node1 = new InMemoryNode(0L, 0L);
        var node2 = new InMemoryNode(1L, 0L);
        store.addNodes(new ArrayList<>(List.of(node1, node2)));
        var rel1 = new InMemoryRelationship(0L, 0L, 1L, relType, 0);
        rel1.addProperty("id", 1);
        store.addRelationships(new ArrayList<>(List.of(rel1)));

        var graph1 = InMemoryGraph.createGraph();
        graph1.updateNode(node1);
        graph1.updateNode(node2);
        graph1.updateRelationship(rel1);

        // Add more nodes and relationships
        var node3 = new InMemoryNode(2L, 42L);
        store.addNodes(new ArrayList<>(List.of(node3)));
        var rel2 = new InMemoryRelationship(1L, 0L, 2L, relType, 42L);
        rel2.addProperty("id", 2);
        var rel3 = new InMemoryRelationship(2L, 1L, 2L, relType, 42L);
        rel3.addProperty("id", 3);
        store.addRelationships(new ArrayList<>(List.of(rel2, rel3)));

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
}
