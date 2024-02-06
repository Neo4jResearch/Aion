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
package org.neo4j.temporalgraph.entities;

import java.nio.ByteBuffer;
import java.util.Map;

public class ByteEntityUtilities {

    private ByteEntityUtilities() {}

    private static final DeserializeInterface[] DE;

    private static final DeserializeInterface2[] DE2;

    static {
        DE = new DeserializeInterface[] {
            ByteEntityUtilities::deserializeNode,
            ByteEntityUtilities::deserializeRelationship,
            (buffer, idsToNames) -> deserializeNeighbourhood(buffer)
        };

        DE2 = new DeserializeInterface2[] {
            ByteEntityUtilities::deserializeNode,
            (buffer, entityId, timestamp, idsToNames) ->
                    deserializeRelationship(buffer, entityId, timestamp, false, idsToNames),
            (buffer, entityId, timestamp, idsToNames) -> deserializeNeighbourhood(buffer, timestamp),
            (buffer, entityId, timestamp, idsToNames) ->
                    deserializeRelationship(buffer, entityId, timestamp, true, idsToNames),
        };
    }

    interface DeserializeInterface {
        InMemoryEntity deserialize(ByteBuffer buffer, Map<Integer, String> idsToNames);
    }

    interface DeserializeInterface2 {
        InMemoryEntity deserialize(ByteBuffer buffer, long entityId, long timestamp, Map<Integer, String> idsToNames);
    }

    public static boolean entityIsDiff(ByteBuffer buffer) {
        var header = buffer.get(buffer.position());
        return getBit(header, 3) == 1;
    }

    public static boolean entityIsDeleted(ByteBuffer buffer) {
        var header = buffer.get(buffer.position());
        return getBit(header, 4) == 1;
    }

    public static long getTimestamp(ByteBuffer buffer) {
        return buffer.getLong(buffer.position() + Byte.BYTES + Long.BYTES);
    }

    public static int getEntityType(ByteBuffer buffer) {
        var header = buffer.get(buffer.position());
        return header & (0b00000111);
    }

    public static void serializeEntity(InMemoryEntity entity, ByteBuffer buffer, Map<String, Integer> namesToIds) {
        serializeEntity(entity, buffer, namesToIds, true);
    }

    public static void serializeEntity(
            InMemoryEntity entity, ByteBuffer buffer, Map<String, Integer> namesToIds, boolean writeIdAndTime) {
        if (entity instanceof InMemoryNode node) {
            serializeEntity(node, buffer, namesToIds, writeIdAndTime);
        } else if (entity instanceof InMemoryRelationship rel) {
            serializeEntity(rel, buffer, namesToIds, writeIdAndTime);
        } else if (entity instanceof InMemoryNeighbourhood neighbourhood) {
            serializeEntity(neighbourhood, buffer, writeIdAndTime);
        } else {
            throw new UnsupportedOperationException(String.format("Entity %s is not supported", entity));
        }
    }

    public static InMemoryEntity deserializeEntity(ByteBuffer buffer, Map<Integer, String> idsToNames) {
        return DE[getEntityType(buffer)].deserialize(buffer, idsToNames);
    }

    public static InMemoryEntity deserializeEntity(
            ByteBuffer buffer, long entityId, long timestamp, Map<Integer, String> idsToNames) {
        return DE2[getEntityType(buffer)].deserialize(buffer, entityId, timestamp, idsToNames);
    }

    public static void serializeEntity(InMemoryNode node, ByteBuffer buffer, Map<String, Integer> namesToIds) {
        serializeEntity(node, buffer, namesToIds, true);
    }

    public static void serializeEntity(
            InMemoryNode node, ByteBuffer buffer, Map<String, Integer> namesToIds, boolean writeIdAndTime) {
        var labels = node.getLabelUpdates();
        var properties = node.getPropertyUpdates();
        int numOfProperties = node.getNumberPropertyUpdates();
        if (labels.size() > Byte.MAX_VALUE || numOfProperties > Byte.MAX_VALUE) {
            throw new OutOfMemoryError(
                    String.format("Cannot store %d labels or %d properties", labels.size(), numOfProperties));
        }

        // Create a header
        buffer.put(createHeader(node));
        if (writeIdAndTime) {
            // Add node id and timestamp
            buffer.putLong(node.getEntityId());
            buffer.putLong(node.getStartTimestamp());
        }

        if (!node.isDeleted()) {
            // Add all labels (assuming the node is not deleted)
            buffer.put((byte) labels.size());
            for (var l : labels) {
                int labelId = namesToIds.get(l.getKey());
                if (!l.getValue()) {
                    labelId = setBit(labelId, 31);
                }
                buffer.putInt(labelId);
            }
            // Add all properties
            addProperties(buffer, properties, numOfProperties, namesToIds);
        }
    }

    public static InMemoryNode deserializeNode(ByteBuffer buffer, Map<Integer, String> idsToNames) {
        var header = buffer.get();
        var nodeId = buffer.getLong();
        var timestamp = buffer.getLong();

        boolean isDiff = (getBit(header, 3) == 1);
        boolean isDeleted = (getBit(header, 4) == 1);

        var node = new InMemoryNode(nodeId, timestamp);
        if (isDeleted) {
            node.setDeleted();
            return node;
        }

        if (isDiff) {
            node.setDiff();
        }
        // Deserialize the labels
        getLabels(node, buffer, idsToNames);
        // Deserialize the properties
        getProperties(node, buffer, idsToNames);

        return node;
    }

    public static InMemoryNode deserializeNode(
            ByteBuffer buffer, long nodeId, long timestamp, Map<Integer, String> idsToNames) {
        var header = buffer.get();

        boolean isDiff = (getBit(header, 3) == 1);
        boolean isDeleted = (getBit(header, 4) == 1);

        var node = new InMemoryNode(nodeId, timestamp);
        if (isDeleted) {
            node.setDeleted();
            return node;
        }

        if (isDiff) {
            node.setDiff();
        }
        // Deserialize the labels
        getLabels(node, buffer, idsToNames);
        // Deserialize the properties
        getProperties(node, buffer, idsToNames);

        return node;
    }

    public static void serializeEntity(
            InMemoryRelationship relationship, ByteBuffer buffer, Map<String, Integer> namesToIds) {
        serializeEntity(relationship, buffer, namesToIds, true);
    }

    public static void serializeEntity(
            InMemoryRelationship relationship,
            ByteBuffer buffer,
            Map<String, Integer> namesToIds,
            boolean writeIdAndTime) {
        var properties = relationship.getPropertyUpdates();
        var numOfProperties = relationship.getNumberPropertyUpdates();
        if (numOfProperties > Byte.MAX_VALUE) {
            throw new OutOfMemoryError(String.format("Cannot store %d properties", numOfProperties));
        }

        // Create a header
        buffer.put(createHeader(relationship));
        if (writeIdAndTime) {
            // Add node id and timestamp
            buffer.putLong(relationship.getEntityId());
            buffer.putLong(relationship.getStartTimestamp());
        }
        // Add start/end nodes
        buffer.putLong(relationship.getStartNode());
        buffer.putLong(relationship.getEndNode());
        // Add all label (assuming the node is not deleted)
        var label = relationship.getType();
        buffer.putInt(label);
        if (!relationship.isDeleted()) {
            // Add pointers if they exist
            if (relationship.hasPointers()) {
                buffer.putLong(relationship.getFirstNextRelId());
                buffer.putLong(relationship.getSecondNextRelId());
                buffer.putLong(relationship.getFirstPrevRelId());
                buffer.putLong(relationship.getSecondPrevRelId());
            }

            // Add all properties
            addProperties(buffer, properties, numOfProperties, namesToIds);
        }
    }

    public static InMemoryRelationship deserializeRelationship(ByteBuffer buffer, Map<Integer, String> idsToNames) {
        var header = buffer.get();
        var nodeId = buffer.getLong();
        var timestamp = buffer.getLong();
        var startNode = buffer.getLong();
        var endNode = buffer.getLong();
        var label = buffer.getInt();

        boolean isDiff = (getBit(header, 3) == 1);
        boolean isDeleted = (getBit(header, 4) == 1);

        var relationship = new InMemoryRelationship(nodeId, startNode, endNode, label, timestamp);
        if (isDeleted) {
            relationship.setDeleted();
            return relationship;
        }

        if (isDiff) {
            relationship.setDiff();
        }
        // Deserialize the properties
        getProperties(relationship, buffer, idsToNames);

        return relationship;
    }

    public static InMemoryRelationship deserializeRelationship(
            ByteBuffer buffer, long relId, long timestamp, boolean withPointers, Map<Integer, String> idsToNames) {
        var header = buffer.get();
        var startNode = buffer.getLong();
        var endNode = buffer.getLong();
        var label = buffer.getInt();

        boolean isDiff = (getBit(header, 3) == 1);
        boolean isDeleted = (getBit(header, 4) == 1);

        var relationship = (!withPointers)
                ? new InMemoryRelationship(relId, startNode, endNode, label, timestamp)
                : new InMemoryRelationshipV2(relId, startNode, endNode, label, timestamp);
        if (isDeleted) {
            relationship.setDeleted();
            return relationship;
        }

        if (isDiff) {
            relationship.setDiff();
        }
        if (withPointers) {
            relationship.setFirstNextRelId(buffer.getLong());
            relationship.setSecondNextRelId(buffer.getLong());
            relationship.setFirstPrevRelId(buffer.getLong());
            relationship.setSecondPrevRelId(buffer.getLong());
        }
        // Deserialize the properties
        getProperties(relationship, buffer, idsToNames);

        return relationship;
    }

    public static void serializeEntity(InMemoryNeighbourhood neighbourhood, ByteBuffer buffer) {
        serializeEntity(neighbourhood, buffer, true);
    }

    public static void serializeEntity(InMemoryNeighbourhood neighbourhood, ByteBuffer buffer, boolean writeIdAndTime) {
        // Create a header
        buffer.put(createHeader(neighbourhood));
        // Add node id and timestamp
        buffer.putLong(neighbourhood.getEntityId());
        if (writeIdAndTime) {
            buffer.putLong(neighbourhood.getStartTimestamp());
        }
    }

    public static InMemoryNeighbourhood deserializeNeighbourhood(ByteBuffer buffer, long timestamp) {
        buffer.rewind();
        var header = buffer.get();
        var relId = buffer.getLong();

        boolean isDeleted = (getBit(header, 4) == 1);

        return new InMemoryNeighbourhood(relId, timestamp, isDeleted);
    }

    public static InMemoryNeighbourhood deserializeNeighbourhood(ByteBuffer buffer) {
        buffer.rewind();
        var header = buffer.get();
        var relId = buffer.getLong();
        var timestamp = buffer.getLong();

        boolean isDeleted = (getBit(header, 4) == 1);

        return new InMemoryNeighbourhood(relId, timestamp, isDeleted);
    }

    private static byte createHeader(InMemoryEntity entity) {
        byte b = 0;
        if (entity.isDeleted()) {
            b = setBit(b, 4);
        } else if (entity.isDiff()) {
            b = setBit(b, 3);
        }

        if (entity instanceof InMemoryNode) {
            b = unsetBit(b, 0);
        } else if (entity instanceof InMemoryRelationship rel) {
            if (rel.hasPointers()) {
                b = setBit(b, 0);
                b = setBit(b, 1);
            } else {
                b = setBit(b, 0);
            }
        } else if (entity instanceof InMemoryNeighbourhood) {
            b = setBit(b, 1);
        } else {
            throw new UnsupportedOperationException(String.format("Entity %s is not supported", entity));
        }

        return b;
    }

    private static void addProperties(
            ByteBuffer buffer, Property[] properties, int numOfProperties, Map<String, Integer> namesToIds) {
        buffer.put((byte) numOfProperties);
        for (int i = 0; i < numOfProperties; ++i) {
            var p = properties[i];
            int propertyId = namesToIds.get(p.name());
            // Deleted property
            if (p.value() == null) {
                propertyId = setBit(propertyId, 31);
                buffer.putInt(propertyId);
            } else {
                switch (p.type()) {
                    case INT -> {
                        propertyId = setByte(propertyId, (byte) 0, 3);
                        buffer.putInt(propertyId);
                        buffer.putInt((int) p.value());
                    }
                    case LONG -> {
                        propertyId = setByte(propertyId, (byte) 1, 3);
                        buffer.putInt(propertyId);
                        buffer.putLong((long) p.value());
                    }
                    case FLOAT -> {
                        propertyId = setByte(propertyId, (byte) 2, 3);
                        buffer.putInt(propertyId);
                        buffer.putFloat((float) p.value());
                    }
                    case DOUBLE -> {
                        propertyId = setByte(propertyId, (byte) 3, 3);
                        buffer.putInt(propertyId);
                        buffer.putDouble((double) p.value());
                    }
                    default -> throw new UnsupportedOperationException(
                            String.format("Type %s is not supported", p.type()));
                }
            }
        }
    }

    private static void getProperties(InMemoryEntity node, ByteBuffer buffer, Map<Integer, String> idsToNames) {
        var numberOfProperties = (int) buffer.get();
        for (int i = 0; i < numberOfProperties; ++i) {
            var propertyId = buffer.getInt();

            // Get metadata about the property type or whether it is deleted
            var isDeleted = (getBit(propertyId, 31) == 1);
            var type = getByte(propertyId, 3);
            propertyId = unsetByte(propertyId, 3);

            var propertyName = idsToNames.get(propertyId);
            if (isDeleted) {
                node.removeProperty(propertyName);
            } else {
                node.addProperty(propertyName, getProperty(buffer, type));
            }
        }
    }

    private static Object getProperty(ByteBuffer buffer, int type) {
        return switch (type) {
            case 0 -> buffer.getInt();
            case 1 -> buffer.getLong();
            case 2 -> buffer.getFloat();
            case 3 -> buffer.getDouble();
            default -> throw new UnsupportedOperationException(String.format("Type %d is not supported", type));
        };
    }

    private static void getLabels(InMemoryNode node, ByteBuffer buffer, Map<Integer, String> idsToNames) {
        var numberOfLabels = (int) buffer.get();
        for (int i = 0; i < numberOfLabels; ++i) {
            var labelId = buffer.getInt();
            var isDeleted = (getBit(labelId, 31) == 1);
            labelId = unsetBit(labelId, 31);

            var labelName = idsToNames.get(labelId);
            if (isDeleted) {
                node.removeLabel(labelName);
            } else {
                node.addLabel(labelName);
            }
        }
    }

    private static byte setBit(byte b, int position) {
        b |= 1 << position;
        return b;
    }

    private static int setBit(int b, int position) {
        b |= 1 << position;
        return b;
    }

    private static int setByte(int num, byte b, int bytePos) {
        num |= b << (bytePos * 8);
        return num;
    }

    private static byte unsetBit(byte b, int position) {
        b &= ~(1 << position);
        return b;
    }

    private static int unsetBit(int b, int position) {
        b &= ~(1 << position);
        return b;
    }

    private static int unsetByte(int num, int bytePos) {
        num &= ~(0xFF << (bytePos * 8));
        return num;
    }

    private static byte getBit(byte value, int position) {
        return (byte) ((value >> position) & 1);
    }

    private static byte getBit(int value, int position) {
        return (byte) ((value >> position) & 1);
    }

    private static byte getByte(int num, int bytePos) {
        return (byte) ((num >> (bytePos * 8)) & 0xFF);
    }
}
