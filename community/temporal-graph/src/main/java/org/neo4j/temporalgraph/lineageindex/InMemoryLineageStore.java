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
package org.neo4j.temporalgraph.lineageindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.entities.TemporalGraph;
import org.neo4j.temporalgraph.lineageindex.entitystores.NodeStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.RelationshipStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.memory.InMemoryNodeStore;
import org.neo4j.temporalgraph.lineageindex.entitystores.memory.LinkedListRelationshipStore;
import org.neo4j.temporalgraph.utils.IntCircularList;
import org.roaringbitmap.RoaringBitmap;

public class InMemoryLineageStore implements LineageStore {
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;

    // Thread local variables for graph retrieval
    private static final ThreadLocal<IntCircularList> bfsQueue = ThreadLocal.withInitial(IntCircularList::new);
    private final ThreadLocal<RoaringBitmap> visitedNodes = ThreadLocal.withInitial(RoaringBitmap::new);

    public InMemoryLineageStore() {
        nodeStore = new InMemoryNodeStore();
        relationshipStore = new LinkedListRelationshipStore();
    }

    public void addNodes(List<InMemoryNode> nodes) throws IOException {
        nodeStore.addNodes(nodes);
    }

    public void addRelationships(List<InMemoryRelationship> rels) throws IOException {
        relationshipStore.addRelationships(rels);
    }

    public List<InMemoryNode> getAllNodes(long timestamp) throws IOException {
        return nodeStore.getAllNodes(timestamp);
    }

    public List<InMemoryRelationship> getAllRelationships(long timestamp) throws IOException {
        return relationshipStore.getAllRelationships(timestamp);
    }

    @Override
    public void flushIndexes() {}

    public void reset() {
        nodeStore.reset();
        relationshipStore.reset();
    }

    @Override
    public Optional<InMemoryNode> getNode(long nodeId, long timestamp) throws IOException {
        return nodeStore.getNode(nodeId, timestamp);
    }

    @Override
    public List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) throws IOException {
        return nodeStore.getNode(nodeId, startTime, endTime);
    }

    @Override
    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) throws IOException {
        return relationshipStore.getRelationship(relId, timestamp);
    }

    @Override
    public List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) throws IOException {
        return relationshipStore.getRelationship(relId, startTime, endTime);
    }

    @Override
    public List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp)
            throws IOException {
        return relationshipStore.getRelationships(nodeId, direction, timestamp);
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) throws IOException {
        return relationshipStore.getRelationships(nodeId, direction, startTime, endTime);
    }

    @Override
    public List<InMemoryNode> expand(long nodeId, RelationshipDirection direction, int hops, long timestamp)
            throws IOException {
        if (direction != RelationshipDirection.OUTGOING) {
            throw new UnsupportedOperationException("Supporting only outgoing edges now");
        }

        var result = new ArrayList<InMemoryNode>();
        var bitmap = visitedNodes.get();
        var queue = bfsQueue.get();
        queue.clear();

        queue.add((int) nodeId);
        for (int i = 0; i < hops; i++) {

            bitmap.clear();
            int queueSize = queue.size();
            for (int j = 0; j < queueSize; ++j) {
                var currentNode = queue.poll();
                var rels = getRelationships(currentNode, direction, timestamp);
                for (var r : rels) {
                    // Outgoing edge
                    if (r.getStartNode() == currentNode) {

                        // Get target node
                        var targetId = (int) r.getEndNode();
                        var targetNode = nodeStore.getNode(targetId, timestamp);
                        if (targetNode.isPresent() && !bitmap.contains(targetId)) {
                            targetNode.get().setHop(i);
                            result.add(targetNode.get());
                            queue.add(targetId);
                            bitmap.add(targetId);
                        }
                    }
                }
            }
        }

        return result;
    }

    @Override
    public List<List<InMemoryNode>> expand(
            long nodeId, RelationshipDirection direction, int hops, long startTime, long endTime, long timeStep)
            throws IOException {
        var result = new ArrayList<List<InMemoryNode>>();
        for (long time = startTime; time <= endTime; time += timeStep) {
            result.add(expand(nodeId, direction, hops, time));
        }
        return result;
    }

    @Override
    public InMemoryGraph getWindow(long startTime, long endTime) throws IOException {
        // Requires allnodes and allrels scan
        throw new IllegalStateException("Implement this method...");
    }

    @Override
    public TemporalGraph getTemporalGraph(long startTime, long endTime) throws IOException {
        throw new IllegalStateException("Implement this method...");
    }

    @Override
    public InMemoryGraph getGraph(long timestamp) throws IOException {
        var nodes = getAllNodes(timestamp);
        var rels = getAllRelationships(timestamp);

        var graph = InMemoryGraph.createGraph();
        for (var n : nodes) {
            graph.updateNode(n);
        }
        for (var r : rels) {
            graph.updateRelationship(r);
        }
        return graph;
    }

    @Override
    public List<InMemoryGraph> getGraph(long startTime, long endTime, long timeStep) throws IOException {
        var result = new ArrayList<InMemoryGraph>();
        for (long time = startTime; time <= endTime; time += timeStep) {
            result.add(getGraph(time));
        }
        return result;
    }

    @Override
    public List<InMemoryEntity> getDiff(long startTime, long endTime) {
        // Requires allnodes and allrels scan
        throw new IllegalStateException("Implement this method...");
    }

    @Override
    public void shutdown() {}
}
