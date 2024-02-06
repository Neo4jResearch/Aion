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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.neo4j.exceptions.InvalidArgumentException;

// todo: use a memory pool
public class InMemoryRelationship implements InMemoryEntity {
    protected long relId;
    protected long startNode;
    protected long endNode;
    protected int type;
    protected long startTimestamp;
    protected long endTimestamp;
    protected final PropertyMap properties;
    protected boolean deleted;
    protected boolean diff;

    private static final String ERROR_MSG = "Unsupported operation.";

    public InMemoryRelationship(long relId, long startNode, long endNode, int type, long timestamp) {
        this(relId, startNode, endNode, type, timestamp, false, false);
    }

    public InMemoryRelationship(
            long relId, long startNode, long endNode, int type, long timestamp, boolean deleted, boolean diff) {
        this.relId = relId;
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;
        this.startTimestamp = timestamp;
        this.endTimestamp = Long.MAX_VALUE;
        this.properties = new PropertyMap();
        this.deleted = deleted;
        this.diff = !this.deleted && diff;
    }

    public void addProperty(String key, Object value) {
        var propertyType = PropertyType.getType(value);
        properties.put(new Property(propertyType, key, value));
    }

    @Override
    public void removeProperty(String key) {
        properties.put(new Property(PropertyType.NULL, key, null));
    }

    @Override
    public long getEntityId() {
        return relId;
    }

    @Override
    public long getStartTimestamp() {
        return startTimestamp;
    }

    @Override
    public long getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public List<Property> getProperties() {
        return new ArrayList<>(properties.getPropertiesAsList());
    }

    public Property[] getPropertyUpdates() {
        return properties.getProperties();
    }

    public int getNumberPropertyUpdates() {
        return properties.size();
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public boolean isDiff() {
        return diff;
    }

    @Override
    public void merge(InMemoryEntity other) {
        if (other instanceof InMemoryRelationship otherRel) {
            this.relId = otherRel.relId;
            this.startTimestamp = otherRel.startTimestamp;
            this.endTimestamp = otherRel.endTimestamp;
            this.deleted = otherRel.deleted;
            this.diff = otherRel.diff;
            this.startNode = otherRel.startNode;
            this.endNode = otherRel.endNode;
            this.type = otherRel.type;

            if (!otherRel.deleted) { // todo: do I need deep copies?
                // Merge properties
                this.properties.putAll(otherRel.properties);
                this.properties.removeDeleted();
            } else {
                this.properties.clear();
            }
        } else {
            throw new InvalidArgumentException(
                    String.format("Cannot merge InMemoryRelationship with %s", other.toString()));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final InMemoryRelationship other = (InMemoryRelationship) obj;
        if (this.relId != other.relId
                || this.deleted != other.deleted
                || this.startTimestamp != other.startTimestamp
                || this.endTimestamp != other.endTimestamp
                || this.diff != other.diff
                || this.startNode != other.startNode
                || this.endNode != other.endNode
                || this.type != other.type) {
            return false;
        }

        // Check properties
        if (!this.properties.equals(other.properties)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(relId)
                ^ Objects.hashCode(startNode)
                ^ Objects.hashCode(endNode)
                ^ Objects.hashCode(type)
                ^ Objects.hashCode(startTimestamp)
                ^ Objects.hashCode(endTimestamp)
                ^ Objects.hashCode(properties)
                ^ Objects.hashCode(deleted)
                ^ Objects.hashCode(diff);
    }

    public boolean equalsWithoutPointers(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final InMemoryRelationship other = (InMemoryRelationship) obj;
        if (this.relId != other.relId
                || this.deleted != other.deleted
                || this.startTimestamp != other.startTimestamp
                || this.endTimestamp != other.endTimestamp
                || this.diff != other.diff
                || this.startNode != other.startNode
                || this.endNode != other.endNode
                || this.type != other.type) {
            return false;
        }

        // Check properties
        if (!this.properties.equals(other.properties)) {
            return false;
        }

        return true;
    }

    public long getStartNode() {
        return startNode;
    }

    public long getEndNode() {
        return endNode;
    }

    public void setFirstNextRelId(long nextRelId) {
        throw new IllegalStateException();
    }

    public long getFirstNextRelId() {
        throw new IllegalStateException(ERROR_MSG);
    }

    public void setSecondNextRelId(long nextRelId) {
        throw new IllegalStateException(ERROR_MSG);
    }

    public long getSecondNextRelId() {
        throw new IllegalStateException(ERROR_MSG);
    }

    public void setFirstPrevRelId(long prevRelId) {
        throw new IllegalStateException(ERROR_MSG);
    }

    public long getFirstPrevRelId() {
        throw new IllegalStateException(ERROR_MSG);
    }

    public void setSecondPrevRelId(long prevRelId) {
        throw new IllegalStateException(ERROR_MSG);
    }

    public void setEntityId(long entityId) {
        this.relId = entityId;
    }

    public void setEndNode(long endNode) {
        this.endNode = endNode;
    }

    public void setStartTimestamp(long timestamp) {
        this.startTimestamp = timestamp;
    }

    @Override
    public void setEndTimestamp(long timestamp) {
        endTimestamp = timestamp;
    }

    /**
     * Update the pointers of the current node during edge deletion to maintain correct
     * references to the edge linked lists.
     */
    public void updateRelId(long prevRelId, long newRelId) {}

    public long getSecondPrevRelId() {
        return -1;
    }

    @Override
    public InMemoryRelationship copy() {
        var newRel = copyWithoutProperties(this.startTimestamp, this.deleted, this.diff);
        newRel.setEndTimestamp(this.getEndTimestamp());
        var props = properties.getProperties();
        for (int i = 0; i < properties.size(); i++) { // todo: deep copy values
            var p = props[i];
            if (p.value() != null) {
                newRel.addProperty(p.name(), p.value());
            } else {
                newRel.removeProperty(p.name());
            }
        }
        return newRel;
    }

    public InMemoryRelationship copyWithoutProperties(long timestamp) {
        return copyWithoutProperties(timestamp, false, false);
    }

    public InMemoryRelationship copyWithoutProperties(long timestamp, boolean deleted, boolean diff) {
        return new InMemoryRelationship(this.relId, this.startNode, this.endNode, this.type, timestamp, deleted, diff);
    }

    @Override
    public void setDiff() {
        this.diff = true;
    }

    @Override
    public void unsetDiff() {
        this.diff = false;
    }

    @Override
    public void setDeleted() {
        this.deleted = true;
    }

    public int getType() {
        return type;
    }

    @Override
    public long getSize() {
        // header + id + timestamp
        var size = Byte.BYTES + Long.BYTES + Long.BYTES;
        // start/end nodes + label
        size += Long.BYTES + Long.BYTES + Integer.BYTES;
        // property counter + properties
        size += Byte.BYTES + properties.size() * (Integer.BYTES + Long.BYTES); // overestimate size
        /*for (var p: properties.values()) {
            size += Integer.BYTES;
            if (p != null) {
                if (p.type() == PropertyType.INT || p.type() == PropertyType.FLOAT) {
                    size += Integer.BYTES;
                } else {
                    size += Long.BYTES;
                }
            }
        }*/

        return size;
    }

    public boolean hasPointers() {
        return false;
    }
}
