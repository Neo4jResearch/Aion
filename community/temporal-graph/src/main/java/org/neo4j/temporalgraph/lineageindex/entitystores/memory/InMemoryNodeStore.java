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
package org.neo4j.temporalgraph.lineageindex.entitystores.memory;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.lineageindex.entitystores.NodeStore;

/*
 * Not thread-safe
 *
 * */
public class InMemoryNodeStore implements NodeStore {
    // (K - V) => K: nodeId, V: node mutations ordered by timestamp
    private final EnhancedTreeMap<Long, InMemoryNode> nodeLineage;

    public InMemoryNodeStore() {
        nodeLineage = new EnhancedTreeMap<>(Comparator.comparingLong(Long::longValue));
    }

    public void addNodes(List<InMemoryNode> nodes) {
        for (var n : nodes) {
            var nodeClone = n.copy();
            nodeLineage.put(nodeClone.getEntityId(), nodeClone);
        }
    }

    public Optional<InMemoryNode> getNode(long nodeId, long timestamp) {
        return nodeLineage.get(nodeId, timestamp);
    }

    public List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) {
        return nodeLineage.rangeScanByTime(nodeId, startTime, endTime);
    }

    public List<InMemoryNode> getAllNodes(long timestamp) {
        return nodeLineage.getAll(timestamp);
    }

    @Override
    public void flushIndex() {}

    public void reset() {
        nodeLineage.reset();
    }

    public void shutdown() {}

    @Override
    public void setDiffThreshold(int threshold) {
        throw new IllegalStateException("Implement this method");
    }
}
