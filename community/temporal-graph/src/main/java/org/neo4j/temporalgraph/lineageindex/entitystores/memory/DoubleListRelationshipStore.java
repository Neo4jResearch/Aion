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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.temporalgraph.entities.InMemoryNeighbourhood;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.entitystores.RelationshipStore;

public class DoubleListRelationshipStore implements RelationshipStore {

    // (K - V) => K: edgeId, V: edge mutations ordered by timestamp
    // The edges create a "temporal" linked list of neighbourhoods for source/target nodes.
    private final EnhancedTreeMap<Long, InMemoryRelationship> relLineage;
    // (K - V) => K: (source, target, relId), V: node's edge list mutations ordered by timestamp
    private final EnhancedTreeMap<Triple<Long, Long, Long>, InMemoryNeighbourhood> outRels;
    // (K - V) => K: (target, source, relId), V: node's edge list mutations ordered by timestamp
    private final EnhancedTreeMap<Triple<Long, Long, Long>, InMemoryNeighbourhood> inRels;

    private static final List<InMemoryRelationship> emptyRelList = List.of();
    private static final List<List<InMemoryRelationship>> emptyRelListList = List.of(List.of());
    private static final List<InMemoryNeighbourhood> emptyNeighbourhoodList = List.of();
    private static final List<List<InMemoryNeighbourhood>> emptyNeighbourhoodListList = List.of(List.of());

    public DoubleListRelationshipStore() {
        relLineage = new EnhancedTreeMap<>(Comparator.comparingLong(Long::longValue));
        inRels = new EnhancedTreeMap<>(new LongTripleComparator());
        outRels = new EnhancedTreeMap<>(new LongTripleComparator());
    }

    @Override
    public void addRelationship(InMemoryRelationship rel) {
        var relClone = rel.copy();
        relLineage.put(relClone.getEntityId(), relClone);

        var sourceId = relClone.getStartNode();
        var targetId = relClone.getEndNode();
        outRels.put(
                ImmutableTriple.of(sourceId, targetId, relClone.getEntityId()),
                new InMemoryNeighbourhood(relClone.getEntityId(), relClone.getStartTimestamp(), relClone.isDeleted()));
        if (sourceId != targetId) {
            inRels.put(
                    ImmutableTriple.of(targetId, sourceId, relClone.getEntityId()),
                    new InMemoryNeighbourhood(
                            relClone.getEntityId(), relClone.getStartTimestamp(), relClone.isDeleted()));
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
        var inNeighbourhood = (direction == RelationshipDirection.OUTGOING)
                ? emptyNeighbourhoodList
                : getInNeighbourhood(nodeId, timestamp);
        var outNeighbourhood = (direction == RelationshipDirection.INCOMING)
                ? emptyNeighbourhoodList
                : getOutNeighbourhood(nodeId, timestamp);
        if (inNeighbourhood.isEmpty() && outNeighbourhood.isEmpty()) {
            return emptyRelList;
        }

        var rels = new ArrayList<InMemoryRelationship>();
        if (direction != RelationshipDirection.OUTGOING) {
            addNeighbourhood(rels, inNeighbourhood, timestamp);
        }
        if (direction != RelationshipDirection.INCOMING) {
            addNeighbourhood(rels, outNeighbourhood, timestamp);
        }
        return rels;
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) {
        var inNeighbourhoods = (direction == RelationshipDirection.OUTGOING)
                ? emptyNeighbourhoodListList
                : getInNeighbourhood(nodeId, startTime, endTime);
        var outNeighbourhoods = (direction == RelationshipDirection.INCOMING)
                ? emptyNeighbourhoodListList
                : getOutNeighbourhood(nodeId, startTime, endTime);
        if (inNeighbourhoods.isEmpty() && outNeighbourhoods.isEmpty()) {
            return emptyRelListList;
        }

        // Get and sort all relationships by timestamp
        List<InMemoryNeighbourhood> neighbourhoods = new ArrayList<>();
        if (direction != RelationshipDirection.OUTGOING) {
            for (var n : inNeighbourhoods) {
                neighbourhoods.addAll(n);
            }
        }
        if (direction != RelationshipDirection.INCOMING) {
            for (var n : outNeighbourhoods) {
                neighbourhoods.addAll(n);
            }
        }
        neighbourhoods.sort(Comparator.comparing(InMemoryNeighbourhood::getStartTimestamp));

        // Go through the relationships and create the final result
        var rels = new ArrayList<List<InMemoryRelationship>>();
        var currentTime = neighbourhoods.get(0).getStartTimestamp();
        Map<Long, InMemoryRelationship> currentRels = new HashMap<>();
        for (var n : neighbourhoods) {
            if (currentTime != n.getStartTimestamp() && !currentRels.isEmpty()) {
                rels.add(currentRels.values().stream().toList());
                currentTime = n.getStartTimestamp();
            }

            var rel = getRelationship(n.getEntityId(), n.getStartTimestamp());
            assert (rel.isPresent());
            if (rel.get().isDeleted()) {
                currentRels.remove(n.getEntityId());
            } else {
                currentRels.put(n.getEntityId(), rel.get());
            }
        }
        if (!currentRels.isEmpty()) {
            rels.add(currentRels.values().stream().toList());
        }

        return rels;
    }

    private void addNeighbourhood(
            List<InMemoryRelationship> result, List<InMemoryNeighbourhood> neighbourhood, long timestamp) {
        for (var n : neighbourhood) {
            if (!n.isDeleted()) {
                var nextRel = relLineage.get(n.getEntityId(), timestamp);
                nextRel.ifPresent(result::add);
            }
        }
    }

    @Override
    public List<InMemoryRelationship> getAllRelationships(long timestamp) {
        return relLineage.getAll(timestamp);
    }

    @Override
    public void reset() {
        relLineage.reset();
        inRels.reset();
        outRels.reset();
    }

    private List<InMemoryNeighbourhood> getInNeighbourhood(long nodeId, long timestamp) {
        var fromKey = ImmutableTriple.of(nodeId, -1L, -1L);
        var toKey = ImmutableTriple.of(nodeId, Long.MAX_VALUE, Long.MAX_VALUE);
        return inRels.rangeScanByKey(fromKey, toKey, timestamp);
    }

    private List<List<InMemoryNeighbourhood>> getInNeighbourhood(long nodeId, long startTime, long endTime) {
        var fromKey = ImmutableTriple.of(nodeId, -1L, -1L);
        var toKey = ImmutableTriple.of(nodeId, Long.MAX_VALUE, Long.MAX_VALUE);
        return inRels.rangeScanByKeyAndTime(fromKey, toKey, startTime, endTime);
    }

    private List<InMemoryNeighbourhood> getOutNeighbourhood(long nodeId, long timestamp) {
        var fromKey = ImmutableTriple.of(nodeId, -1L, -1L);
        var toKey = ImmutableTriple.of(nodeId, Long.MAX_VALUE, Long.MAX_VALUE);
        return outRels.rangeScanByKey(fromKey, toKey, timestamp);
    }

    private List<List<InMemoryNeighbourhood>> getOutNeighbourhood(long nodeId, long startTime, long endTime) {
        var fromKey = ImmutableTriple.of(nodeId, -1L, -1L);
        var toKey = ImmutableTriple.of(nodeId, Long.MAX_VALUE, Long.MAX_VALUE);
        return outRels.rangeScanByKeyAndTime(fromKey, toKey, startTime, endTime);
    }

    static class LongTripleComparator implements Comparator<Triple<Long, Long, Long>> {
        @Override
        public int compare(final Triple<Long, Long, Long> lhs, final Triple<Long, Long, Long> rhs) {
            final var left = lhs.getLeft().compareTo(rhs.getLeft());
            if (left == 0) {
                final var middle = lhs.getMiddle().compareTo(rhs.getMiddle());
                if (middle == 0) {
                    return lhs.getRight().compareTo(rhs.getRight());
                }
                return middle;
            }
            return left;
        }
    }

    public int numberOfRelationships() {
        int result = 0;
        for (var e : relLineage.tree.entrySet()) {
            result += e.getValue().size();
        }
        return result;
    }

    public int numberOfNeighbourhoodRecords() {
        int result = 0;
        for (var e : inRels.tree.entrySet()) {
            result += e.getValue().size();
        }
        for (var e : outRels.tree.entrySet()) {
            result += e.getValue().size();
        }
        return result;
    }

    @Override
    public void flushIndexes() {}

    @Override
    public void shutdown() {}

    @Override
    public void setDiffThreshold(int threshold) {
        throw new IllegalStateException("Implement this method");
    }
}
