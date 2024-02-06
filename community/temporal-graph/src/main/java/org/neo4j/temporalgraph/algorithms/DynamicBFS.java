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
package org.neo4j.temporalgraph.algorithms;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.utils.IntCircularList;
import org.roaringbitmap.RoaringBitmap;

public class DynamicBFS implements DynamicAlgorithm<List<Pair<Integer, Integer>>> {
    private InMemoryGraph graph; // todo: the graph should be updated externally
    private List<Pair<Integer, Integer>> result;
    private final long source;
    MutableIntList distances;
    IntCircularList frontier;
    private static final int DEFAULT_VALUE = Integer.MAX_VALUE;

    public DynamicBFS(long nodeId) {
        this.source = nodeId;
    }

    @Override
    public void initialize(InMemoryGraph graph) {
        this.graph = graph;
        frontier = new IntCircularList();
        bfs();
    }

    @Override
    public void update(List<InMemoryEntity> graphUpdates) {
        RoaringBitmap taggedNodes = new RoaringBitmap();
        RoaringBitmap affectedNodes = new RoaringBitmap();
        for (var update : graphUpdates) {
            if (update instanceof InMemoryNode node) {
                if (!node.isDeleted() && node.getEntityId() >= distances.size()) {
                    resizeDistances((int) node.getEntityId());
                }
                if (node.isDeleted()) {
                    var nodeId = (int) node.getEntityId();
                    for (var rel : graph.getRelationships(nodeId, RelationshipDirection.OUTGOING)) {
                        var dest = (int) rel.getEndNode();
                        if (propagationCondition(nodeId, dest)) {
                            taggedNodes.add(dest);
                        }
                    }
                    for (var rel : graph.getRelationships(nodeId, RelationshipDirection.INCOMING)) {
                        var src = (int) rel.getEndNode();
                        if (propagationCondition(src, nodeId)) {
                            taggedNodes.add(src);
                        }
                    }
                }
                graph.updateNode(node);
            } else if (update instanceof InMemoryRelationship rel) {
                var dest = (int) rel.getEndNode();
                if (!rel.isDeleted()) {
                    affectedNodes.add(dest);
                } else {
                    var src = rel.getStartNode();
                    if (propagationCondition(src, dest)) {
                        taggedNodes.add(dest);
                    }
                }
                graph.updateRelationship(rel);
            } else {
                throw new IllegalArgumentException(String.format("Invalid update type %s", update));
            }
        }

        // First reset tagged nodes and propagate changes
        if (!taggedNodes.isEmpty()) {
            var taggedQueue = new IntCircularList(taggedNodes.stream().toArray());
            propagateTags(taggedQueue, taggedNodes, affectedNodes);
        }

        // Perform incremental execution on affected nodes
        incrementalBfs(new IntCircularList(affectedNodes.stream().toArray()));
    }

    @Override
    public List<Pair<Integer, Integer>> getResult() {
        if (result == null) {
            throw new IllegalStateException("BFS is not computed yet");
        }
        return result;
    }

    @Override
    public void reset() {
        graph = null;
        result = null;
    }

    private void bfs() {
        long maxNodeId = Integer.MIN_VALUE;
        var nodeIterator = graph.getNodeMap().iterator();
        while (nodeIterator.hasNext()) {
            var node = nodeIterator.next();
            maxNodeId = Math.max(maxNodeId, node.getEntityId() + 1);
        }
        initializeDistances((int) maxNodeId); // assumes node ids are between [0, |nodes|-1]

        frontier.clear();
        frontier.add((int) source);

        int distance = 0;
        distances.set((int) source, distance); // todo: remove casting
        while (!frontier.isEmpty()) {
            distance++;
            var frontierSize = frontier.size();
            for (int i = 0; i < frontierSize; ++i) {
                var src = frontier.poll();
                for (var rel : graph.getRelationships(src, RelationshipDirection.OUTGOING)) {
                    var dest = rel.getEndNode();
                    if (updateFunction((int) dest, distance)) {
                        frontier.add((int) dest);
                    }
                }
            }
        }

        // Store only valid distances
        result = new ArrayList<>();
        for (int i = 0; i < distances.size(); ++i) {
            if (graph.getNodeMap().contains(i)) {
                result.add(new ImmutablePair<>(i, distances.get(i)));
            }
        }
    }

    private void propagateTags(IntCircularList taggedNodes, RoaringBitmap seen, RoaringBitmap affectedNodes) {
        while (!taggedNodes.isEmpty()) {
            var src = taggedNodes.poll();
            for (var rel : graph.getRelationships(src, RelationshipDirection.OUTGOING)) {
                var dest = (int) rel.getEndNode();
                if (!seen.contains(dest) && propagationCondition(src, dest)) {
                    taggedNodes.add(dest);
                }
            }
            distances.set(src, DEFAULT_VALUE);
            affectedNodes.add(src);
        }
    }

    private void incrementalBfs(IntCircularList affectedNodes) {
        while (!affectedNodes.isEmpty()) {
            var dest = affectedNodes.poll();
            for (var rel : graph.getRelationships(dest, RelationshipDirection.INCOMING)) {
                var src = rel.getStartNode();
                selectionFunction(src, dest, affectedNodes);
            }
        }

        // Store only valid distances
        result = new ArrayList<>();
        for (int i = 0; i < distances.size(); ++i) {
            if (graph.getNodeMap().contains(i)) {
                result.add(new ImmutablePair<>(i, distances.get(i)));
            }
        }
    }

    private void selectionFunction(long source, long destination, IntCircularList affectedNodes) {
        var sourceValue = distances.get((int) source);
        var newValue = (sourceValue == Integer.MAX_VALUE) ? Integer.MAX_VALUE : sourceValue + 1;
        var destinationValue = distances.get((int) destination);
        if (newValue < destinationValue) {
            distances.set((int) destination, sourceValue + 1);
            affectedNodes.add((int) destination);
        }
    }

    private boolean updateFunction(int pos, int distance) {
        if (distances.get(pos) > distance) {
            distances.set(pos, distance);
            return true;
        }
        return false;
    }

    private boolean propagationCondition(long src, long dest) {
        return (distances.get((int) src) + 1 == distances.get((int) dest));
    }

    void initializeDistances(int size) {
        distances = IntLists.mutable.empty();
        for (int i = 0; i < size; ++i) {
            distances.add(DEFAULT_VALUE);
        }
    }

    void resizeDistances(int size) {
        var newDistances = IntLists.mutable.empty();
        newDistances.addAll(distances);
        for (int i = distances.size(); i < size; ++i) {
            newDistances.add(DEFAULT_VALUE);
        }
        distances = newDistances;
    }
}
