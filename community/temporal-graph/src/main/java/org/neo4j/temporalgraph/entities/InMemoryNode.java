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

import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.exceptions.InvalidArgumentException;

// todo: use a memory pool
public class InMemoryNode implements InMemoryEntity {
    private long nodeId;
    private long startTimestamp;
    private long endTimestamp;
    private final PropertyMap properties;
    private final LabelMap labels;
    private boolean deleted;
    private boolean diff;
    private int hop;

    public InMemoryNode(long nodeId, long timestamp) {
        this(nodeId, timestamp, false, false);
    }

    public InMemoryNode(long nodeId, long timestamp, boolean deleted, boolean diff) {
        this.nodeId = nodeId;
        this.startTimestamp = timestamp;
        this.endTimestamp = Long.MAX_VALUE;
        this.properties = new PropertyMap();
        this.labels = new LabelMap();
        this.deleted = deleted;
        this.diff = !this.deleted && diff;
        this.hop = 0;
    }

    public void addProperty(String key, Object value) {
        var type = PropertyType.getType(value);
        properties.put(new Property(type, key, value));
    }

    @Override
    public void removeProperty(String key) {
        properties.put(new Property(PropertyType.NULL, key, null));
    }

    public void addLabel(String label) {
        labels.put(label, true);
    }

    public void removeLabel(String label) {
        labels.put(label, false);
    }

    public List<String> getLabels() {
        return labels.getLabelsAsList();
    }

    public List<Pair<String, Boolean>> getLabelUpdates() {
        return labels.getLabels();
    }

    @Override
    public long getEntityId() {
        return nodeId;
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
    public void setEndTimestamp(long timestamp) {
        endTimestamp = timestamp;
    }

    @Override
    public List<Property> getProperties() {
        return properties.getPropertiesAsList();
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
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final InMemoryNode other = (InMemoryNode) obj;
        if (this.nodeId != other.nodeId
                || this.deleted != other.deleted
                || this.diff != other.diff
                || this.startTimestamp != other.startTimestamp
                || this.endTimestamp != other.endTimestamp) {
            return false;
        }

        // Check labels
        if (!this.labels.equals(other.labels)) {
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
        return Objects.hashCode(nodeId)
                ^ Objects.hashCode(startTimestamp)
                ^ Objects.hashCode(endTimestamp)
                ^ Objects.hashCode(properties)
                ^ Objects.hashCode(labels)
                ^ Objects.hashCode(deleted)
                ^ Objects.hashCode(diff);
    }

    @Override
    public void merge(InMemoryEntity other) {
        if (other instanceof InMemoryNode otherNode) {
            this.nodeId = otherNode.nodeId;
            this.startTimestamp = otherNode.startTimestamp;
            this.endTimestamp = otherNode.endTimestamp;
            this.deleted = otherNode.deleted;
            this.diff = otherNode.diff;

            if (!otherNode.deleted) { // todo: do I need deep copies?
                // Merge labels
                this.labels.putAll(otherNode.labels);
                this.labels.removeDeleted();

                // Merge properties
                this.properties.putAll(otherNode.properties);
                this.properties.removeDeleted();
            } else {
                this.labels.clear();
                this.properties.clear();
            }
        } else {
            throw new InvalidArgumentException(String.format("Cannot merge InMemoryNode with %s", other.toString()));
        }
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

    @Override
    public InMemoryNode copy() {
        var newNode = new InMemoryNode(this.nodeId, this.startTimestamp, this.deleted, this.diff);
        newNode.setEndTimestamp(this.getEndTimestamp());
        var props = properties.getProperties();
        for (int i = 0; i < properties.size(); i++) { // todo: deep copy values
            var p = props[i];
            if (p.value() != null) {
                newNode.addProperty(p.name(), p.value());
            } else {
                newNode.removeProperty(p.name());
            }
        }
        var ls = labels.getLabels();
        for (int i = 0; i < labels.size(); i++) {
            var l = ls.get(i);
            if (Boolean.TRUE.equals(l.getValue())) {
                newNode.addLabel(l.getKey());
            } else {
                newNode.removeLabel(l.getKey());
            }
        }

        return newNode;
    }

    @Override
    public long getSize() {
        // header + id + timestamp
        var size = Byte.BYTES + Long.BYTES + Long.BYTES;
        // label counter + labels
        size += Byte.BYTES + labels.size() * Integer.BYTES;
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

    public void setHop(int hop) {
        this.hop = hop;
    }

    public int getHop() {
        return hop;
    }
}
