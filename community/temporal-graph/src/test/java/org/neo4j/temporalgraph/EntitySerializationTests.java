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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.temporalgraph.entities.ByteEntityUtilities;
import org.neo4j.temporalgraph.entities.InMemoryNeighbourhood;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;

@ExtendWith({TestDirectorySupportExtension.class})
class EntitySerializationTests {

    @Test
    void shouldSerializeInMemoryNodes() {
        Map<String, Integer> namesToIds = new HashMap<>();
        namesToIds.put("Node", 0);
        namesToIds.put("prop1", 1);
        namesToIds.put("prop2", 2);

        Map<Integer, String> idsToNames = new HashMap<>();
        idsToNames.put(0, "Node");
        idsToNames.put(1, "prop1");
        idsToNames.put(2, "prop2");

        // Test a fully materialized node
        var node1 = new InMemoryNode(123L, 42L);
        node1.addLabel("Node");
        node1.addProperty("prop1", 7);
        node1.addProperty("prop2", 13);

        ByteBuffer buffer = ByteBuffer.allocate(128);
        ByteEntityUtilities.serializeEntity(node1, buffer, namesToIds);
        assertEquals(node1, ByteEntityUtilities.deserializeNode(buffer.rewind(), idsToNames));

        // Test a node diff
        var node2 = new InMemoryNode(123L, 128L, false, true);
        node2.removeLabel("Node");
        node2.addProperty("prop1", 14);
        node2.removeProperty("prop2");

        buffer.clear();
        ByteEntityUtilities.serializeEntity(node2, buffer, namesToIds);
        assertEquals(node2, ByteEntityUtilities.deserializeNode(buffer.rewind(), idsToNames));

        // Test a node deletion
        var node3 = new InMemoryNode(123L, 128L, true, false);

        buffer.clear();
        ByteEntityUtilities.serializeEntity(node3, buffer, namesToIds);
        assertEquals(node3, ByteEntityUtilities.deserializeNode(buffer.rewind(), idsToNames));
    }

    @Test
    void shouldSerializeInMemoryRelationships() {
        Map<String, Integer> namesToIds = new HashMap<>();
        namesToIds.put("Relationship", 0);
        namesToIds.put("prop1", 1);
        namesToIds.put("prop2", 2);

        Map<Integer, String> idsToNames = new HashMap<>();
        idsToNames.put(0, "Relationship");
        idsToNames.put(1, "prop1");
        idsToNames.put(2, "prop2");

        var startNode = 3L;
        var endNode = 30L;
        var relType = 64;

        // Test a fully materialized relationship
        var rel1 = new InMemoryRelationship(123L, startNode, endNode, relType, 42L);
        rel1.addProperty("prop1", 7);
        rel1.addProperty("prop2", 13);

        ByteBuffer buffer = ByteBuffer.allocate(128);
        ByteEntityUtilities.serializeEntity(rel1, buffer, namesToIds);
        assertEquals(rel1, ByteEntityUtilities.deserializeRelationship(buffer.rewind(), idsToNames));

        // Test a relationship diff
        var rel2 = new InMemoryRelationship(123L, startNode, endNode, relType, 42L, false, true);
        rel2.addProperty("prop1", 14);
        rel2.removeProperty("prop2");

        buffer.clear();
        ByteEntityUtilities.serializeEntity(rel2, buffer, namesToIds);
        assertEquals(rel2, ByteEntityUtilities.deserializeRelationship(buffer.rewind(), idsToNames));

        // Test a relationship deletion
        var rel3 = new InMemoryRelationship(123L, startNode, endNode, relType, 42L, true, false);

        buffer.clear();
        ByteEntityUtilities.serializeEntity(rel3, buffer, namesToIds);
        assertEquals(rel3, ByteEntityUtilities.deserializeRelationship(buffer.rewind(), idsToNames));
    }

    @Test
    void shouldSerializeInMemoryNeighbourhoods() {
        // Test a fully materialized neighbourhood
        InMemoryNeighbourhood neighbourhood1 = new InMemoryNeighbourhood(123L, 42L, false);

        ByteBuffer buffer = ByteBuffer.allocate(128);
        ByteEntityUtilities.serializeEntity(neighbourhood1, buffer);
        assertEquals(neighbourhood1, ByteEntityUtilities.deserializeNeighbourhood(buffer.rewind()));

        // Test a neighbourhood deletion
        var neighbourhood2 = new InMemoryNeighbourhood(123L, 42L, true);

        buffer.clear();
        ByteEntityUtilities.serializeEntity(neighbourhood2, buffer);
        assertEquals(neighbourhood2, ByteEntityUtilities.deserializeNeighbourhood(buffer.rewind()));
    }

    @Test
    void shouldDeserializedMultipleEntities() {
        Map<String, Integer> namesToIds = new HashMap<>();
        namesToIds.put("Node", 0);
        namesToIds.put("Relationship", 1);
        namesToIds.put("prop1", 2);
        namesToIds.put("prop2", 3);

        Map<Integer, String> idsToNames = new HashMap<>();
        idsToNames.put(0, "Node");
        idsToNames.put(1, "Relationship");
        idsToNames.put(2, "prop1");
        idsToNames.put(3, "prop2");

        var node1 = new InMemoryNode(123L, 42L);
        node1.addLabel("Node");
        node1.addProperty("prop1", 7);
        node1.addProperty("prop2", 13);

        var rel1 = new InMemoryRelationship(123L, 3L, 30L, 64, 42L);
        rel1.addProperty("prop1", 7);
        rel1.addProperty("prop2", 13);

        var node2 = new InMemoryNode(123L, 128L, false, true);
        node2.removeLabel("Node");
        node2.addProperty("prop1", 14);
        node2.removeProperty("prop2");

        var neighbourhood1 = new InMemoryNeighbourhood(123L, 42L, false);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteEntityUtilities.serializeEntity(node1, buffer, namesToIds);
        ByteEntityUtilities.serializeEntity(rel1, buffer, namesToIds);
        ByteEntityUtilities.serializeEntity(node2, buffer, namesToIds);
        ByteEntityUtilities.serializeEntity(neighbourhood1, buffer, namesToIds);
        buffer.rewind();

        assertEquals(node1, ByteEntityUtilities.deserializeEntity(buffer, idsToNames));
        assertEquals(rel1, ByteEntityUtilities.deserializeEntity(buffer, idsToNames));
        assertEquals(node2, ByteEntityUtilities.deserializeEntity(buffer, idsToNames));
        assertEquals(neighbourhood1, ByteEntityUtilities.deserializeEntity(buffer, idsToNames));
    }
}
