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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.neo4j.temporalgraph.entities.InMemoryNeighbourhood;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.InMemoryRelationshipV2;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.entitystores.RelationshipStore;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/*
 * Not thread-safe
 *
 * */
public class LinkedListRelationshipStore implements RelationshipStore {
    // (K - V) => K: edgeId, V: edge mutations ordered by timestamp
    // The edges create a "temporal" linked list of neighbourhoods for source/target nodes.
    private final EnhancedTreeMap<Long, InMemoryRelationship> relLineage;
    // (K - V) => K: nodeId, V: node's edge list mutations ordered by timestamp
    // Given a nodeId, this tree will return a pointer to the "temporal" linked list from above.
    private final EnhancedTreeMap<Long, InMemoryNeighbourhood> neighbourhoodLineage;

    private static final List<InMemoryRelationship> emptyRelList = List.of();

    private static final ThreadLocal<Roaring64Bitmap> visitedNodes = ThreadLocal.withInitial(Roaring64Bitmap::new);

    public LinkedListRelationshipStore() {
        relLineage = new EnhancedTreeMap<>(Comparator.comparingLong(Long::longValue));
        neighbourhoodLineage = new EnhancedTreeMap<>(Comparator.comparingLong(Long::longValue));
    }

    @Override
    public void addRelationship(InMemoryRelationship rel) {
        var relClone = InMemoryRelationshipV2.createCopy(rel);
        if (relClone.isDeleted()) {
            handleDeletion(relClone);
        } else {
            addSingleRelationship(relClone);
        }
    }

    @Override
    public void addRelationships(List<InMemoryRelationship> rels) {
        for (var r : rels) {
            addRelationship(r);
        }
    }

    @Override
    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) {
        return relLineage.get(relId, timestamp);
    }

    @Override
    public List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) {
        return relLineage.rangeScanByTime(relId, startTime, endTime);
    }

    @Override
    public List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp) {
        var neighbourhood = neighbourhoodLineage.get(nodeId, timestamp);
        if (neighbourhood.isEmpty() || neighbourhood.get().isDeleted()) {
            return emptyRelList;
        }

        return getRelationshipsFromNeighbourhood(neighbourhood.get(), nodeId, direction, timestamp);
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) {
        var neighbourhoods = neighbourhoodLineage.rangeScanByTime(nodeId, startTime, endTime);

        var result = new ArrayList<List<InMemoryRelationship>>();
        for (var n : neighbourhoods) {
            result.add(getRelationshipsFromNeighbourhood(n, nodeId, direction, n.getStartTimestamp()));
        }
        return result;
    }

    private List<InMemoryRelationship> getRelationshipsFromNeighbourhood(
            InMemoryNeighbourhood neighbourhood, long nodeId, RelationshipDirection direction, long timestamp) {
        var rels = new ArrayList<InMemoryRelationship>();
        var nextRelId = neighbourhood.getEntityId();
        while (nextRelId != -1) {
            var nextRel = relLineage.get(nextRelId, timestamp);

            nextRelId = -1;
            if (nextRel.isPresent() && nextRel.get().getStartTimestamp() <= timestamp) {
                var actualRel = nextRel.get();

                if (checkDirection(nodeId, actualRel, direction)) {
                    rels.add(actualRel);
                }

                // Get next relationship pointer from the linked list
                if (actualRel.getStartNode() == nodeId) {
                    nextRelId = actualRel.getFirstPrevRelId();
                } else {
                    nextRelId = actualRel.getSecondPrevRelId();
                }
            }
        }
        return rels;
    }

    private boolean checkDirection(long nodeId, InMemoryRelationship rel, RelationshipDirection direction) {
        return direction == RelationshipDirection.BOTH
                || (direction == RelationshipDirection.INCOMING && rel.getEndNode() == nodeId)
                || (direction == RelationshipDirection.OUTGOING && rel.getStartNode() == nodeId);
    }

    @Override
    public List<InMemoryRelationship> getAllRelationships(long timestamp) {
        return relLineage.getAll(timestamp);
    }

    @Override
    public void reset() {
        relLineage.reset();
        neighbourhoodLineage.reset();
    }

    @Override
    public int numberOfRelationships() {
        int result = 0;
        for (var e : relLineage.tree.entrySet()) {
            result += e.getValue().size();
        }
        return result;
    }

    @Override
    public int numberOfNeighbourhoodRecords() {
        int result = 0;
        for (var e : neighbourhoodLineage.tree.entrySet()) {
            result += e.getValue().size();
        }
        return result;
    }

    @Override
    public void flushIndexes() {}

    private void addSingleRelationship(InMemoryRelationshipV2 r) {
        // First, get the previous edge lists from src and trg nodes and update their pointers
        updateEdgePointers(r); // todo: do I need to create new edge copies here? Or do the timestamps protect accesses?

        // Then, add the relationship to the edge lineage
        insertOrMergeRelationship(r);

        // Finally, if this is a new insertion, update the node neighbourhoods.
        if (!r.isDiff()) {
            updateNeighbourhoods(r);
        }
    }

    private void updateNeighbourhoods(InMemoryRelationship r) {
        insertOrUpdateNeighbourhood(r.getStartNode(), r.getEntityId(), r.getStartTimestamp(), r.isDeleted());
        if (r.getStartNode() != r.getEndNode()) {
            insertOrUpdateNeighbourhood(r.getEndNode(), r.getEntityId(), r.getStartTimestamp(), r.isDeleted());
        }
    }

    private void insertOrUpdateNeighbourhood(long nodeId, long relId, long timestamp, boolean deleted) {
        var prevEntry = neighbourhoodLineage.getLastEntry(nodeId);
        if (prevEntry.isPresent() && prevEntry.get().getStartTimestamp() == timestamp && !deleted) {
            prevEntry.get().setEntityId(relId);
            prevEntry.get().setDeleted(false);
        } else {
            neighbourhoodLineage.put(nodeId, new InMemoryNeighbourhood(relId, timestamp, deleted));
        }
    }

    private void handleDeletion(InMemoryRelationshipV2 r) {
        // Get previous relationship and update
        var prevRel = relLineage.getLastEntry(r.getEntityId());
        if (prevRel.isEmpty()) {
            throw new IllegalStateException("Cannot delete a non existing edge");
        }
        // Add the new value
        updateOrDeleteRelationship(r);
        updateNeighbourhoods(r);

        // Update all the pointers in the linked lists
        var actualRel = prevRel.get();
        var timestamp = r.getStartTimestamp();
        // Copy and reconnect the linked lists by removing the node in the middle.
        // In addition, update the neighbourhoods, starting from the previous pointers
        // to get a valid neighbourhood.

        // Keep track of updates relationships to re-insert them only once.
        var visited = visitedNodes.get();
        visited.clear();

        if (actualRel.getFirstPrevRelId() != -1) {
            var nodeId = actualRel.getFirstPrevRelId();
            var node = (InMemoryRelationshipV2) relLineage.getLastEntry(nodeId).get();
            if (!node.isDeleted()) {
                var newNode = node.copyWithoutProperties(timestamp, node.isDeleted(), true);
                newNode.updateRelId(r.getEntityId(), actualRel.getFirstNextRelId());
                insertOrMergeRelationship(newNode);
                updateNeighbourhoods(newNode);
            }
            visited.add(nodeId);
        }

        if (actualRel.getSecondPrevRelId() != -1) {
            var nodeId = actualRel.getSecondPrevRelId();
            if (!visited.contains(nodeId)) {
                var node = relLineage.getLastEntry(nodeId).get();
                if (!node.isDeleted()) {
                    var newNode =
                            (InMemoryRelationshipV2) node.copyWithoutProperties(timestamp, node.isDeleted(), true);
                    newNode.updateRelId(r.getEntityId(), actualRel.getSecondNextRelId());
                    insertOrMergeRelationship(newNode);
                    updateNeighbourhoods(newNode);
                }
                visited.add(nodeId);
            }
        }

        if (actualRel.getFirstNextRelId() != -1) {
            var nodeId = actualRel.getFirstNextRelId();
            if (!visited.contains(nodeId)) {
                var node = relLineage.getLastEntry(nodeId).get();
                if (!node.isDeleted()) {
                    var newNode =
                            (InMemoryRelationshipV2) node.copyWithoutProperties(timestamp, node.isDeleted(), true);
                    newNode.updateRelId(r.getEntityId(), actualRel.getFirstPrevRelId());
                    insertOrMergeRelationship(newNode);
                    updateNeighbourhoods(newNode);
                }
                visited.add(nodeId);
            }
        }

        if (actualRel.getSecondNextRelId() != -1) {
            var nodeId = actualRel.getSecondNextRelId();
            if (!visited.contains(nodeId)) {
                var node = relLineage.getLastEntry(nodeId).get();
                if (!node.isDeleted()) {
                    var newNode =
                            (InMemoryRelationshipV2) node.copyWithoutProperties(timestamp, node.isDeleted(), true);
                    newNode.updateRelId(r.getEntityId(), actualRel.getSecondPrevRelId());
                    insertOrMergeRelationship(newNode);
                    updateNeighbourhoods(newNode);
                }
            }
        }
    }

    private void updateOrDeleteRelationship(InMemoryRelationship r) {
        if (!relLineage.setDeleted(r.getEntityId(), r.getStartTimestamp())) {
            relLineage.put(r.getEntityId(), r);
        }
    }

    private void insertOrMergeRelationship(InMemoryRelationshipV2 r) {
        var prevRel = relLineage.get(r.getEntityId(), r.getStartTimestamp());
        if (prevRel.isEmpty() || prevRel.get().getStartTimestamp() != r.getStartTimestamp()) {
            relLineage.put(r.getEntityId(), r);
        } else {
            prevRel.get().merge(r);
        }
    }

    /**
     *
     * Assuming that the edges of a node create a logical linked list, when updating an existing edge,
     * we perform CoW.
     *
     * @param r is the new relationship added to relLineage
     */
    private void updateEdgePointers(InMemoryRelationshipV2 r) {
        // If the edge already exits, copy its pointers
        var prevRel = relLineage.getLastEntry(r.getEntityId());
        if (prevRel.isPresent()) {
            r.copyPointers((InMemoryRelationshipV2) prevRel.get());
        } else {
            updateEdgePointersForInsertion(r);
        }
    }

    private void updateEdgePointersForInsertion(InMemoryRelationshipV2 r) {
        // Get the previous edge that belongs to the source node
        var prevSourceEdge = neighbourhoodLineage.get(r.getStartNode(), r.getStartTimestamp());
        if (prevSourceEdge.isPresent()) {
            // Update it next pointer to the new edge if it's not the same as r
            var prevEdgeId = prevSourceEdge.get().getEntityId();
            if (prevEdgeId != r.getEntityId()) {
                var prevEdge = relLineage.get(prevEdgeId, r.getStartTimestamp()).get();
                // Check if we just added this relationship with this timestamp
                boolean addEdge = prevEdge.getStartTimestamp() != r.getStartTimestamp();
                var prevEdgeCopy = (addEdge) ? prevEdge.copy() : prevEdge;
                if (prevEdgeCopy.getStartNode() == r.getStartNode()) {
                    prevEdgeCopy.setFirstNextRelId(r.getEntityId());
                } else if (prevEdgeCopy.getEndNode() == r.getStartNode()) {
                    prevEdgeCopy.setSecondNextRelId(r.getEntityId());
                } else {
                    throw new IllegalStateException("Invalid state while updating relationship pointers");
                }
                // Update the timestamp and add the new copy back to the relationships
                if (addEdge) {
                    // Do I need to update the timestamp?
                    // prevEdgeCopy.setTimestamp(r.getTimestamp());
                    relLineage.put(prevEdgeId, prevEdgeCopy);
                    if (!prevEdge.isDiff()) {
                        prevEdgeCopy.unsetDiff();
                    }
                }

                // Set current's edge previous pointer
                r.setFirstPrevRelId(prevEdgeId);
            }
        }

        // Repeat the same symmetrically for the target node
        if (r.getStartNode() != r.getEndNode()) {
            var prevTargetEdge = neighbourhoodLineage.get(r.getEndNode(), r.getStartTimestamp());
            if (prevTargetEdge.isPresent()) {
                // Update it next pointer to the new edge if it's not the same as r
                var prevEdgeId = prevTargetEdge.get().getEntityId();
                if (prevEdgeId != r.getEntityId()) {
                    var prevEdge =
                            relLineage.get(prevEdgeId, r.getStartTimestamp()).get();
                    // Check if we just added this relationship with this timestamp
                    boolean addEdge = prevEdge.getStartTimestamp() != r.getStartTimestamp();
                    var prevEdgeCopy = (addEdge) ? prevEdge.copy() : prevEdge;
                    if (prevEdgeCopy.getStartNode() == r.getEndNode()) {
                        prevEdgeCopy.setFirstNextRelId(r.getEntityId());
                    } else if (prevEdgeCopy.getEndNode() == r.getEndNode()) {
                        prevEdgeCopy.setSecondNextRelId(r.getEntityId());
                    } else {
                        throw new IllegalStateException("Invalid state while updating relationship pointers");
                    }
                    // Update the timestamp and add the new copy back to the relationships
                    if (addEdge) {
                        // Do I need to update the timestamp?
                        // prevEdgeCopy.setTimestamp(r.getTimestamp());
                        relLineage.put(prevEdgeId, prevEdgeCopy);
                        if (!prevEdge.isDiff()) {
                            prevEdgeCopy.unsetDiff();
                        }
                    }
                    // Set current's edge previous pointer
                    r.setSecondPrevRelId(prevEdgeId);
                }
            }
        }
    }

    @Override
    public void shutdown() {}

    @Override
    public void setDiffThreshold(int threshold) {
        throw new IllegalStateException("Implement this method");
    }
}
