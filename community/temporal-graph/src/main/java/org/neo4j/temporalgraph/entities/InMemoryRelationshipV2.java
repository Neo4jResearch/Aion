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

import java.util.Objects;
import org.neo4j.exceptions.InvalidArgumentException;

// todo: use a memory pool
public class InMemoryRelationshipV2 extends InMemoryRelationship {
    private long firstNextRelId;
    private long secondNextRelId;
    private long firstPrevRelId;
    private long secondPrevRelId;

    public InMemoryRelationshipV2(long relId, long startNode, long endNode, int type, long timestamp) {
        this(relId, startNode, endNode, type, timestamp, false, false);
    }

    public InMemoryRelationshipV2(
            long relId, long startNode, long endNode, int type, long timestamp, boolean deleted, boolean diff) {
        super(relId, startNode, endNode, type, timestamp, deleted, diff);
        this.firstNextRelId = -1L;
        this.firstPrevRelId = -1L;
        this.secondNextRelId = -1L;
        this.secondPrevRelId = -1L;
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

            if (other instanceof InMemoryRelationshipV2 otherRelV2) {
                copyPointers(otherRelV2);
            }
        } else {
            throw new InvalidArgumentException(
                    String.format("Cannot merge InMemoryRelationshipV2 with %s", other.toString()));
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

        final InMemoryRelationshipV2 other = (InMemoryRelationshipV2) obj;
        if (this.relId != other.relId
                || this.deleted != other.deleted
                || this.startTimestamp != other.startTimestamp
                || this.endTimestamp != other.endTimestamp
                || this.diff != other.diff
                || this.startNode != other.startNode
                || this.endNode != other.endNode
                || this.type != other.type
                || this.firstPrevRelId != other.firstPrevRelId
                || this.firstNextRelId != other.firstNextRelId
                || this.secondPrevRelId != other.secondPrevRelId
                || this.secondNextRelId != other.secondNextRelId) {
            return false;
        }

        // Check properties
        if (!this.properties.equals(other.properties)) {
            return false;
        }

        return true;
    }

    @Override
    public boolean equalsWithoutPointers(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass() && !(obj instanceof InMemoryRelationship)) {
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
                ^ Objects.hashCode(firstNextRelId)
                ^ Objects.hashCode(secondNextRelId)
                ^ Objects.hashCode(firstPrevRelId)
                ^ Objects.hashCode(secondPrevRelId)
                ^ Objects.hashCode(deleted)
                ^ Objects.hashCode(diff);
    }

    @Override
    public void setFirstNextRelId(long nextRelId) {
        this.firstNextRelId = nextRelId;
    }

    @Override
    public long getFirstNextRelId() {
        return firstNextRelId;
    }

    @Override
    public void setSecondNextRelId(long nextRelId) {
        this.secondNextRelId = nextRelId;
    }

    @Override
    public long getSecondNextRelId() {
        return secondNextRelId;
    }

    @Override
    public void setFirstPrevRelId(long prevRelId) {
        this.firstPrevRelId = prevRelId;
    }

    @Override
    public long getFirstPrevRelId() {
        return firstPrevRelId;
    }

    @Override
    public void setSecondPrevRelId(long prevRelId) {
        this.secondPrevRelId = prevRelId;
    }

    /**
     * Update the pointers of the current node during edge deletion to maintain correct
     * references to the edge linked lists.
     */
    @Override
    public void updateRelId(long prevRelId, long newRelId) {
        firstPrevRelId = (firstPrevRelId == prevRelId) ? newRelId : firstPrevRelId;
        firstNextRelId = (firstNextRelId == prevRelId) ? newRelId : firstNextRelId;
        secondPrevRelId = (secondPrevRelId == prevRelId) ? newRelId : secondPrevRelId;
        secondNextRelId = (secondNextRelId == prevRelId) ? newRelId : secondNextRelId;
    }

    @Override
    public long getSecondPrevRelId() {
        return secondPrevRelId;
    }

    public void copyPointers(InMemoryRelationshipV2 other) {
        this.firstPrevRelId = other.firstPrevRelId;
        this.firstNextRelId = other.firstNextRelId;
        this.secondPrevRelId = other.secondPrevRelId;
        this.secondNextRelId = other.secondNextRelId;
    }

    @Override
    public InMemoryRelationshipV2 copy() {
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

    @Override
    public InMemoryRelationshipV2 copyWithoutProperties(long timestamp) {
        return copyWithoutProperties(timestamp, false, false);
    }

    @Override
    public InMemoryRelationshipV2 copyWithoutProperties(long timestamp, boolean deleted, boolean diff) {
        var newRel = new InMemoryRelationshipV2(
                this.relId, this.startNode, this.endNode, this.type, timestamp, deleted, diff);
        newRel.copyPointers(this);
        return newRel;
    }

    @Override
    public long getSize() {
        // header + id + timestamp
        var size = Byte.BYTES + Long.BYTES + Long.BYTES;
        // pointers
        size += 4 * Long.BYTES;
        // start/end nodes + label
        size += Long.BYTES + Long.BYTES + Integer.BYTES;
        // property counter + properties
        size += Byte.BYTES + properties.size() * (Integer.BYTES + Long.BYTES); // overestimate size
        return size;
    }

    @Override
    public boolean hasPointers() {
        return firstNextRelId != -1 || firstPrevRelId != -1 || secondNextRelId != -1 || secondPrevRelId != -1;
    }

    public static InMemoryRelationshipV2 createCopy(InMemoryRelationship rel) {
        var newRel = new InMemoryRelationshipV2(
                rel.relId, rel.startNode, rel.endNode, rel.type, rel.startTimestamp, rel.deleted, rel.diff);
        newRel.setEndTimestamp(rel.getEndTimestamp());
        var props = rel.properties.getProperties();
        for (int i = 0; i < rel.properties.size(); i++) { // todo: deep copy values
            var p = props[i];
            if (p.value() != null) {
                newRel.addProperty(p.name(), p.value());
            } else {
                newRel.removeProperty(p.name());
            }
        }
        return newRel;
    }
}
