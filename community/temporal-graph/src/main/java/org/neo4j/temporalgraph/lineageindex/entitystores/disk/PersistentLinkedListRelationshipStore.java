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
package org.neo4j.temporalgraph.lineageindex.entitystores.disk;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.temporalgraph.entities.InMemoryNeighbourhood;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.InMemoryRelationshipV2;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.entitystores.RelationshipStore;
import org.roaringbitmap.longlong.Roaring64Bitmap;

public class PersistentLinkedListRelationshipStore implements RelationshipStore {

    // Key: Pair<relId, timestamp>, Value: RawBytes
    private final EntityStore<InMemoryRelationship> relLineage;

    // Key: Pair<nodeId, timestamp>, Value: RawBytes
    // Given a nodeId, this tree will return a pointer to the "temporal" linked list from above.
    private final EntityStore<InMemoryNeighbourhood> neighbourhoodLineage;

    private final ThreadLocal<Roaring64Bitmap> visitedNodes = ThreadLocal.withInitial(Roaring64Bitmap::new);

    private static final List<InMemoryRelationship> emptyRelList = List.of();

    public PersistentLinkedListRelationshipStore(
            PageCache pageCache,
            FileSystemAbstraction fs,
            Path relIndexPath,
            Map<String, Integer> namesToIds,
            Map<Integer, String> idsToNames) {
        relLineage = new EntityStore<>(pageCache, fs, relIndexPath, namesToIds, idsToNames);

        var neighbourhoodPath = relIndexPath.getParent().resolve("NEIGHBOURHOODS");
        neighbourhoodLineage = new EntityStore<>(pageCache, fs, neighbourhoodPath, namesToIds, idsToNames);
    }

    @Override
    public void addRelationship(InMemoryRelationship rel) throws IOException {
        var relClone = InMemoryRelationshipV2.createCopy(rel); // todo: remove copy
        if (relClone.isDeleted()) {
            handleDeletion(relClone);
        } else {
            addSingleRelationship(relClone);
        }
    }

    @Override
    public void addRelationships(List<InMemoryRelationship> rels) throws IOException {
        // todo: merge relationships
        for (var r : rels) {
            addRelationship(r);
        }
    }

    @Override
    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) throws IOException {
        return relLineage.get(relId, timestamp);
    }

    @Override
    public List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) throws IOException {
        return relLineage.get(relId, startTime, endTime);
    }

    @Override
    public List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp)
            throws IOException {
        var neighbourhood = neighbourhoodLineage.get(nodeId, timestamp);
        if (neighbourhood.isEmpty() || neighbourhood.get().isDeleted()) {
            return emptyRelList;
        }

        return getRelationshipsFromNeighbourhood(neighbourhood.get(), nodeId, direction, timestamp);
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) throws IOException {
        var neighbourhoods = neighbourhoodLineage.get(nodeId, startTime, endTime);

        var result = new ArrayList<List<InMemoryRelationship>>();
        for (var n : neighbourhoods) {
            result.add(getRelationshipsFromNeighbourhood(n, nodeId, direction, n.getStartTimestamp()));
        }
        return result;
    }

    private List<InMemoryRelationship> getRelationshipsFromNeighbourhood(
            InMemoryNeighbourhood neighbourhood, long nodeId, RelationshipDirection direction, long timestamp)
            throws IOException {
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
    public List<InMemoryRelationship> getAllRelationships(long timestamp) throws IOException {
        return relLineage.getAll(timestamp);
    }

    @Override
    public void reset() {
        // todo: reset relLineage, neighbourhoodLineage
    }

    public int numberOfRelationships() throws IOException {
        return relLineage.numberOfEntities();
    }

    public int numberOfNeighbourhoodRecords() throws IOException {
        return neighbourhoodLineage.numberOfEntities();
    }

    @Override
    public void flushIndexes() {
        relLineage.flushIndex();
        neighbourhoodLineage.flushIndex();
    }

    @Override
    public void shutdown() throws IOException {
        relLineage.shutdown();
        neighbourhoodLineage.shutdown();
    }

    @Override
    public void setDiffThreshold(int threshold) {
        throw new IllegalStateException("Implement this method");
    }

    private void addSingleRelationship(InMemoryRelationshipV2 r) throws IOException {
        // First, get the previous edge lists from src and trg nodes and update their pointers
        updateEdgePointers(r); // todo: do I need to create new edge copies here? Or do the timestamps protect accesses?

        // Then, add the relationship to the edge lineage
        relLineage.put(r);

        // Finally, if this is a new insertion, update the node neighbourhoods.
        if (!r.isDiff()) {
            updateNeighbourhoods(r);
        }
    }

    private void handleDeletion(InMemoryRelationshipV2 r) throws IOException {
        // Get previous relationship and update
        var prevRel = relLineage.get(r.getEntityId(), Long.MAX_VALUE);
        if (prevRel.isEmpty()) {
            throw new IllegalStateException("Cannot delete a non existing edge");
        }
        // Create a relationship with pointers if this is not already the case
        if (!(prevRel.get() instanceof InMemoryRelationshipV2)) {
            prevRel = Optional.of(InMemoryRelationshipV2.createCopy(prevRel.get()));
        }
        // Add the new value
        relLineage.put(r);
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
            var relId = actualRel.getFirstPrevRelId();
            deletionUpdate(timestamp, relId, r.getEntityId(), actualRel.getFirstNextRelId());
            visited.add(relId);
        }

        if (actualRel.getSecondPrevRelId() != -1) {
            var relId = actualRel.getSecondPrevRelId();
            if (!visited.contains(relId)) {
                deletionUpdate(timestamp, relId, r.getEntityId(), actualRel.getSecondNextRelId());
                visited.add(relId);
            }
        }

        if (actualRel.getFirstNextRelId() != -1) {
            var relId = actualRel.getFirstNextRelId();
            if (!visited.contains(relId)) {
                deletionUpdate(timestamp, relId, r.getEntityId(), actualRel.getFirstPrevRelId());
                visited.add(relId);
            }
        }

        if (actualRel.getSecondNextRelId() != -1) {
            var relId = actualRel.getSecondNextRelId();
            if (!visited.contains(relId)) {
                deletionUpdate(timestamp, relId, r.getEntityId(), actualRel.getSecondPrevRelId());
            }
        }
    }

    private void deletionUpdate(long timestamp, long relId, long prevRelId, long newRelId) throws IOException {
        var relFromIndex = relLineage.get(relId, Long.MAX_VALUE);
        if (relFromIndex.isPresent() && !relFromIndex.get().isDeleted()) {
            var newRel = relFromIndex.get().copyWithoutProperties(timestamp);
            newRel.updateRelId(prevRelId, newRelId);
            newRel.setDiff();
            relLineage.put(newRel.getEntityId(), newRel);
            updateNeighbourhoods(newRel);
        }
    }

    private void updateNeighbourhoods(InMemoryRelationship r) throws IOException {
        neighbourhoodLineage.put(
                r.getStartNode(), new InMemoryNeighbourhood(r.getEntityId(), r.getStartTimestamp(), r.isDeleted()));
        if (r.getStartNode() != r.getEndNode()) {
            neighbourhoodLineage.put(
                    r.getEndNode(), new InMemoryNeighbourhood(r.getEntityId(), r.getStartTimestamp(), r.isDeleted()));
        }
    }

    /**
     *
     * Assuming that the edges of a node create a logical linked list, when updating an existing edge,
     * we perform CoW.
     *
     * @param r is the new relationship added to relLineage
     */
    private void updateEdgePointers(InMemoryRelationshipV2 r) throws IOException {
        // If the edge already exits, copy its pointers
        var prevRel = relLineage.get(r.getEntityId(), Long.MAX_VALUE);
        if (prevRel.isPresent()) {
            r.copyPointers((InMemoryRelationshipV2) prevRel.get());
        } else {
            updateEdgePointersForInsertion(r);
        }
    }

    private void updateEdgePointersForInsertion(InMemoryRelationshipV2 r) throws IOException {
        // Get the previous edge that belongs to the source node
        var prevSourceEdge = neighbourhoodLineage.get(r.getStartNode(), r.getStartTimestamp());
        if (prevSourceEdge.isPresent()) {
            // Update it next pointer to the new edge if it's not the same as r
            var prevEdgeId = prevSourceEdge.get().getEntityId();
            if (prevEdgeId != r.getEntityId()) {
                var prevEdge = relLineage.get(prevEdgeId, r.getStartTimestamp()).get();
                // Create a relationship with pointers if this is not already the case
                if (!(prevEdge instanceof InMemoryRelationshipV2)) {
                    prevEdge = InMemoryRelationshipV2.createCopy(prevEdge);
                }
                if (prevEdge.getStartNode() == r.getStartNode()) {
                    prevEdge.setFirstNextRelId(r.getEntityId());
                } else if (prevEdge.getEndNode() == r.getStartNode()) {
                    prevEdge.setSecondNextRelId(r.getEntityId());
                } else {
                    throw new IllegalStateException("Invalid state while updating relationship pointers");
                }
                // Store back the result
                relLineage.put(prevEdge.getEntityId(), prevEdge);

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
                    // Create a relationship with pointers if this is not already the case
                    if (!(prevEdge instanceof InMemoryRelationshipV2)) {
                        prevEdge = InMemoryRelationshipV2.createCopy(prevEdge);
                    }
                    if (prevEdge.getStartNode() == r.getEndNode()) {
                        prevEdge.setFirstNextRelId(r.getEntityId());
                    } else if (prevEdge.getEndNode() == r.getEndNode()) {
                        prevEdge.setSecondNextRelId(r.getEntityId());
                    } else {
                        throw new IllegalStateException("Invalid state while updating relationship pointers");
                    }
                    // Store back the result
                    relLineage.put(prevEdge.getEntityId(), prevEdge);

                    // Set current's edge previous pointer
                    r.setSecondPrevRelId(prevEdgeId);
                }
            }
        }
    }
}
