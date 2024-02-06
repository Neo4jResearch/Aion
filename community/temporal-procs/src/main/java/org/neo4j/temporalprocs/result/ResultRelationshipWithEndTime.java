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
package org.neo4j.temporalprocs.result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.temporalgraph.entities.Property;

public class ResultRelationshipWithEndTime {
    public long startTimestamp;
    public long endTimestamp;
    public long relId;
    public long startNode;
    public long endNode;
    public long type;
    public Map<String, Object> properties;

    public ResultRelationshipWithEndTime(
            long startTime,
            long endTime,
            long relId,
            long startNode,
            long endNode,
            long type,
            List<Property> properties) {
        this.startTimestamp = startTime;
        this.endTimestamp = endTime;
        this.relId = relId;
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;

        // todo: deep copy labels/properties?
        this.properties = new HashMap<>();
        for (var p : properties) {
            this.properties.put(p.name(), p.value());
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

        final ResultRelationshipWithEndTime other = (ResultRelationshipWithEndTime) obj;
        if (this.relId != other.relId
                || this.startTimestamp != other.startTimestamp
                || this.endTimestamp != other.endTimestamp
                || this.startNode != other.startNode
                || this.endNode != other.endNode
                || this.type != other.type) {
            return false;
        }

        // Check properties
        if (this.properties.size() != other.properties.size()) {
            return false;
        }
        for (var entry : this.properties.entrySet()) {
            if (!other.properties.containsKey(entry.getKey())
                    || !other.properties.get(entry.getKey()).equals(entry.getValue())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(startTimestamp)
                ^ Objects.hashCode(endTimestamp)
                ^ Objects.hashCode(relId)
                ^ Objects.hashCode(startNode)
                ^ Objects.hashCode(endNode)
                ^ Objects.hashCode(type)
                ^ Objects.hashCode(properties);
    }
}
