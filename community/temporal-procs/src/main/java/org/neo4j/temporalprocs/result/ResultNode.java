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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.temporalgraph.entities.Property;

public class ResultNode {
    public long timestamp;
    public long nodeId;
    public List<String> labels;
    public Map<String, Object> properties;

    public ResultNode(long timestamp, long nodeId, List<String> labels, List<Property> properties) {
        this.timestamp = timestamp;
        this.nodeId = nodeId;

        // todo: deep copy labels/properties?
        this.labels = new ArrayList<>(labels);

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

        final ResultNode other = (ResultNode) obj;
        if (this.nodeId != other.nodeId || this.timestamp != other.timestamp) {
            return false;
        }

        // Check labels
        if (this.labels.size() != other.labels.size()) {
            return false;
        }
        for (var entry : this.labels) {
            if (!other.labels.contains(entry)) {
                return false;
            }
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
        return Objects.hashCode(timestamp)
                ^ Objects.hashCode(nodeId)
                ^ Objects.hashCode(properties)
                ^ Objects.hashCode(labels);
    }
}
