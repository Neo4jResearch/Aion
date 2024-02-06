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
import static org.neo4j.temporalgraph.TestUtils.getPageCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.entitystores.RelationshipStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.disk.PersistentDoubleListRelationshipStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.disk.PersistentLinkedListRelationshipStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.memory.DoubleListRelationshipStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.memory.LinkedListRelationshipStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith({TestDirectorySupportExtension.class})
class RelationshipStoreTests {

    @Inject
    private TestDirectory directory;

    private static final int relType = 42;
    private static final String NODE_STORE_INDEX = "nsindex";
    private static final String REL_STORE_INDEX = "rsindex";
    private static final Map<String, Integer> namesToIds = new HashMap<>();
    private static final Map<Integer, String> idsToNames = new HashMap<>();

    @BeforeAll
    static void setup() {
        namesToIds.put("name", 0);
        idsToNames.put(0, "name");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void shouldReturnCorrectRelationships(int type) throws IOException {
        var store = storeOfType(type);

        // Add relationships at timestamp 0L
        var timestamp = 0L;
        var rels = new ArrayList<>(Arrays.asList(
                new InMemoryRelationship(0L, 0, 1, relType, timestamp),
                new InMemoryRelationship(1L, 0, 2, relType, timestamp),
                new InMemoryRelationship(2L, 2, 1, relType, timestamp)));
        store.addRelationships(rels);

        // Check whether rels exist based on a valid timestamp and id
        for (var r : rels) {
            checkRelationshipExists(store, r.getEntityId(), 0L, r);
            checkRelationshipExists(store, r.getEntityId(), 42L, r);
        }

        // Check that no rel is returned for invalid node id
        checkRelationshipNotPresent(store, 42L, 0L);

        // Check that rel don't exist with a timestamp smaller tha 0
        checkRelationshipNotPresent(store, 0L, -1L);
        checkRelationshipNotPresent(store, 1L, -1L);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void shouldReturnCorrectRelationshipsWithDeletions(int type) throws IOException {
        var store = storeOfType(type);

        // Add relationships at timestamp1
        var timestamp1 = 0L;
        var rels1 = new ArrayList<>(Arrays.asList(
                new InMemoryRelationship(0L, 0, 1, relType, timestamp1),
                new InMemoryRelationship(1L, 0, 2, relType, timestamp1),
                new InMemoryRelationship(2L, 2, 1, relType, timestamp1)));
        for (var r : rels1) {
            r.addProperty("name", r.getEntityId());
        }
        store.addRelationships(rels1);

        // Add properties to relationships
        var timestamp2 = 42L;
        var rels2 = new ArrayList<>(Arrays.asList(
                new InMemoryRelationship(0L, 0, 1, relType, timestamp2, false, true),
                new InMemoryRelationship(1L, 0, 2, relType, timestamp2, false, true),
                new InMemoryRelationship(2L, 2, 1, relType, timestamp2, false, true)));
        for (var r : rels2) {
            r.addProperty("name", 2 * r.getEntityId());
        }
        store.addRelationships(rels2);

        // Delete relationships at timestamp3
        var timestamp3 = 128L;
        var rels3 = new ArrayList<>(Arrays.asList(
                new InMemoryRelationship(1L, 0, 2, relType, timestamp3, true, false),
                new InMemoryRelationship(2L, 2, 1, relType, timestamp3, true, false)));
        store.addRelationships(rels3);

        // Check whether rels exist based on a valid timestamp and id
        for (var r : rels1) {
            checkRelationshipExists(store, r.getEntityId(), timestamp1, r);
        }

        // Check that the properties are updated
        for (var r : rels2) {
            checkRelationshipExists(store, r.getEntityId(), timestamp2, r);
        }

        // Check that rels don't exist after deletion
        for (var r : rels3) {
            checkRelationshipNotPresent(store, r.getEntityId(), timestamp3);
        }
        // Check the relationship still remaining
        checkRelationshipExists(store, rels2.get(0).getEntityId(), timestamp2, rels2.get(0));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void shouldReturnCorrectNodeRelationships(int type) throws IOException {
        var store = storeOfType(type);

        // Add relationships at timestamp1
        var timestamp1 = 0L;
        var rels1 = new ArrayList<>(Arrays.asList(
                new InMemoryRelationship(0L, 0, 1, relType, timestamp1),
                new InMemoryRelationship(1L, 0, 2, relType, timestamp1),
                new InMemoryRelationship(2L, 2, 1, relType, timestamp1)));
        for (var r : rels1) {
            r.addProperty("name", r.getEntityId());
        }
        store.addRelationships(rels1);

        // Add properties to relationships
        var timestamp2 = 42L;
        var rels2 = new ArrayList<>(Arrays.asList(
                new InMemoryRelationship(0L, 0, 1, relType, timestamp2, false, true),
                new InMemoryRelationship(1L, 0, 2, relType, timestamp2, false, true),
                new InMemoryRelationship(2L, 2, 1, relType, timestamp2, false, true)));
        for (var r : rels2) {
            r.addProperty("name", 2 * r.getEntityId());
        }
        store.addRelationships(rels2);

        // Delete relationships at timestamp3
        var timestamp3 = 128L;
        var rels3 = new ArrayList<>(Arrays.asList(
                new InMemoryRelationship(1L, 0, 2, relType, timestamp3, true, false),
                new InMemoryRelationship(3L, 2, 3, relType, timestamp3, false, false)));
        store.addRelationships(rels3);

        // Check whether rels exist based on a valid timestamp and nodeId
        var nodeRels1 = store.getRelationships(2, RelationshipDirection.BOTH, timestamp1);
        nodeRels1.sort(Comparator.comparing(InMemoryRelationship::getEntityId));
        assertTrue(nodeRels1.get(0).equalsWithoutPointers(rels1.get(1)));
        assertTrue(nodeRels1.get(1).equalsWithoutPointers(rels1.get(2)));

        // Check that the properties are updated
        var nodeRels2 = store.getRelationships(2, RelationshipDirection.BOTH, timestamp2);
        nodeRels2.sort(Comparator.comparing(InMemoryRelationship::getEntityId));
        assertTrue(nodeRels2.get(0).equalsWithoutPointers(rels2.get(1)));
        assertTrue(nodeRels2.get(1).equalsWithoutPointers(rels2.get(2)));

        // Check that rels don't exist after deletion
        var nodeRels3 = store.getRelationships(2, RelationshipDirection.BOTH, timestamp3);
        nodeRels3.sort(Comparator.comparing(InMemoryRelationship::getEntityId));
        nodeRels3.get(0).setStartTimestamp(timestamp2);
        assertTrue(nodeRels3.get(0).equalsWithoutPointers(rels2.get(2)));
        assertTrue(nodeRels3.get(1).equalsWithoutPointers(rels3.get(1)));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void shouldReturnAllRelationships(int type) throws IOException {
        // Create a store with 100_000 relationships.
        // Then, add 100_000 relationships again, and finally delete 50_000.
        // It should return all the visible relationships according to the given timestamp.
        var store = storeOfType(type);

        final int batchSize = 100_000;

        // Add relationships at timestamp1
        var timestamp1 = 42L;
        List<InMemoryRelationship> relsAtTimestamp1 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            relsAtTimestamp1.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp1));
        }
        store.addRelationships(relsAtTimestamp1);

        // Add relationships at timestamp2
        var timestamp2 = 128L;
        List<InMemoryRelationship> relsAtTimestamp2 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 2 >= batchSize) ? i + 2 - batchSize : i + 2;
            relsAtTimestamp2.add(new InMemoryRelationship(i + batchSize, startNode, endNode, relType, timestamp2));
        }
        store.addRelationships(relsAtTimestamp2);

        // Remove relationships from timestamp1
        var timestamp3 = 256L;
        List<InMemoryRelationship> relsAtTimestamp3 = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            var startNode = i;
            var endNode = (i + 1 == batchSize) ? 0 : i + 1;
            relsAtTimestamp3.add(new InMemoryRelationship(i, startNode, endNode, relType, timestamp3, true, false));
        }
        store.addRelationships(relsAtTimestamp3);

        // Check rels before timestamp1
        var resultRels1 = store.getAllRelationships(0L);
        assertEquals(0, resultRels1.size());

        // Check nodes at timestamp1
        var resultRels2 = store.getAllRelationships(timestamp1);
        assertEquals(batchSize, resultRels2.size());
        // The first 100_000 rels
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    relsAtTimestamp1.get(i).getEntityId(), resultRels2.get(i).getEntityId());
            assertEquals(
                    relsAtTimestamp1.get(i).getStartNode(), resultRels2.get(i).getStartNode());
            assertEquals(
                    relsAtTimestamp1.get(i).getEndNode(), resultRels2.get(i).getEndNode());
        }

        // Check nodes at timestamp2
        var resultRels3 = store.getAllRelationships(timestamp2);
        assertEquals(2 * batchSize, resultRels3.size());
        // The first 100_000 rels
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    relsAtTimestamp1.get(i).getEntityId(), resultRels3.get(i).getEntityId());
            assertEquals(
                    relsAtTimestamp1.get(i).getStartNode(), resultRels3.get(i).getStartNode());
            assertEquals(
                    relsAtTimestamp1.get(i).getEndNode(), resultRels3.get(i).getEndNode());
        }
        // The second 100_000 rels
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    relsAtTimestamp2.get(i).getEntityId(),
                    resultRels3.get(i + batchSize).getEntityId());
            assertEquals(
                    relsAtTimestamp2.get(i).getStartNode(),
                    resultRels3.get(i + batchSize).getStartNode());
            assertEquals(
                    relsAtTimestamp2.get(i).getEndNode(),
                    resultRels3.get(i + batchSize).getEndNode());
        }

        // Check nodes at timestamp3
        var resultRels4 = store.getAllRelationships(timestamp3);
        assertEquals(batchSize, resultRels4.size());
        // The remaining 100_000 rels
        for (int i = 0; i < batchSize; ++i) {
            assertEquals(
                    relsAtTimestamp2.get(i).getEntityId(), resultRels4.get(i).getEntityId());
            assertEquals(
                    relsAtTimestamp2.get(i).getStartNode(), resultRels4.get(i).getStartNode());
            assertEquals(
                    relsAtTimestamp2.get(i).getEndNode(), resultRels4.get(i).getEndNode());
        }
    }

    private void checkRelationshipExists(RelationshipStore store, long relId, long timestamp, InMemoryRelationship rel)
            throws IOException {
        var resultRel = store.getRelationship(relId, timestamp);
        assertTrue(resultRel.isPresent());
        assertTrue(resultRel.get().equalsWithoutPointers(rel));
    }

    private void checkRelationshipNotPresent(RelationshipStore store, long relId, long timestamp) throws IOException {
        var resultRel = store.getRelationship(relId, timestamp);
        assertTrue(resultRel.isEmpty());
    }

    private RelationshipStore storeOfType(int type) {
        return switch (type) {
            case 0 -> new LinkedListRelationshipStore();
            case 1 -> new DoubleListRelationshipStore();
            case 2 -> new PersistentLinkedListRelationshipStore(
                    getPageCache(directory),
                    directory.getFileSystem(),
                    directory.homePath().resolve(REL_STORE_INDEX),
                    namesToIds,
                    idsToNames);
            case 3 -> new PersistentDoubleListRelationshipStore(
                    getPageCache(directory),
                    directory.getFileSystem(),
                    directory.homePath().resolve(REL_STORE_INDEX),
                    namesToIds,
                    idsToNames);
            default -> throw new IllegalArgumentException(String.format("Not supported type %d", type));
        };
    }
}
