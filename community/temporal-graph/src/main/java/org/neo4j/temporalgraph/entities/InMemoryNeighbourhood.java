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
import org.neo4j.exceptions.InvalidArgumentException;

public class InMemoryNeighbourhood implements InMemoryEntity {
    private long relId;
    private long startTimestamp;
    private long endTimestamp;
    private boolean deleted;

    private static final String NO_PROPERTIES_MESSAGE = "A neighbourhood does not have any properties.";

    public InMemoryNeighbourhood(long relId, long startTimestamp, boolean deleted) {
        this.relId = relId;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = Long.MAX_VALUE;
        this.deleted = deleted;
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
        throw new UnsupportedOperationException(NO_PROPERTIES_MESSAGE);
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public boolean isDiff() {
        return false;
    }

    @Override
    public void merge(InMemoryEntity other) {

        if (other instanceof InMemoryNeighbourhood otherNode) {
            this.startTimestamp = otherNode.startTimestamp;
            this.deleted = otherNode.deleted;
            this.relId = otherNode.relId;
        } else {
            throw new InvalidArgumentException(
                    String.format("Cannot merge InMemoryNeighbourhood with %s", other.toString()));
        }
    }

    @Override
    public void addProperty(String key, Object value) {
        throw new UnsupportedOperationException(NO_PROPERTIES_MESSAGE);
    }

    @Override
    public void removeProperty(String key) {
        throw new UnsupportedOperationException(NO_PROPERTIES_MESSAGE);
    }

    @Override
    public void setDiff() {}

    @Override
    public void unsetDiff() {}

    @Override
    public void setDeleted() {}

    @Override
    public void setEndTimestamp(long timestamp) {
        endTimestamp = timestamp;
    }

    public void setEntityId(long relId) {
        this.relId = relId;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public InMemoryNeighbourhood copy() {
        return new InMemoryNeighbourhood(this.relId, this.startTimestamp, this.deleted);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final InMemoryNeighbourhood other = (InMemoryNeighbourhood) obj;
        return (relId == other.relId
                && startTimestamp == other.startTimestamp
                && endTimestamp == other.endTimestamp
                && deleted == other.deleted);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(relId)
                ^ Objects.hashCode(startTimestamp)
                ^ Objects.hashCode(endTimestamp)
                ^ Objects.hashCode(deleted);
    }

    @Override
    public long getSize() {
        return (long) Byte.BYTES + Long.BYTES + Long.BYTES;
    }
}
