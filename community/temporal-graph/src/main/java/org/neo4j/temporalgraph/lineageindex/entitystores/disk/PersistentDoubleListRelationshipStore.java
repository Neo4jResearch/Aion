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

import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.temporalgraph.entities.InMemoryNeighbourhood;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.entitystores.RelationshipStore;

public class PersistentDoubleListRelationshipStore implements RelationshipStore {

    // Key: Pair<relId, timestamp>, Value: RawBytes
    private final EntityStore<InMemoryRelationship> relLineage;

    // K: Quad<source, target, timestamp, edgeId>, V: null
    // We use a quadruple to have unique keys. In the first bit of the edgeId, we store whether it's deleted
    private final GBPTree<MutableQuad<Long, Long, Long, Long>, NullValue> outRels;
    // K: Quad<target, source, timestamp, edgeId>, V: null
    // We use a quadruple to have unique keys. In the first bit of the edgeId, we store whether it's deleted
    private final GBPTree<MutableQuad<Long, Long, Long, Long>, NullValue> inRels;
    private final ThreadLocal<List<MutableQuad<Long, Long, Long, Long>>> eKeys =
            ThreadLocal.withInitial(() -> Arrays.asList(new MutableQuad<>(), new MutableQuad<>()));

    private final ThreadLocal<Map<Long, InMemoryRelationship>> relMap = ThreadLocal.withInitial(HashMap::new);

    private static final List<InMemoryRelationship> emptyRelList = List.of();
    private static final List<List<InMemoryRelationship>> emptyRelListList = List.of(List.of());
    private static final List<InMemoryNeighbourhood> emptyNeighbourhoodList = List.of();
    private static final List<Long> emptyLongList = List.of();

    public PersistentDoubleListRelationshipStore(
            PageCache pageCache,
            FileSystemAbstraction fs,
            Path relIndexPath,
            Map<String, Integer> namesToIds,
            Map<Integer, String> idsToNames) {
        relLineage = new EntityStore<>(pageCache, fs, relIndexPath, namesToIds, idsToNames);

        ImmutableSet<OpenOption> openOptions = Sets.immutable.empty();
        var inRelsPath = relIndexPath.getParent().resolve("IN_RELATIONSHIPS");
        inRels = new GBPTreeBuilder<>(pageCache, fs, inRelsPath, new NeighbourhoodIndexLayout())
                .with(openOptions)
                .build();
        var outRelsPath = relIndexPath.getParent().resolve("OUT_RELATIONSHIPS");
        outRels = new GBPTreeBuilder<>(pageCache, fs, outRelsPath, new NeighbourhoodIndexLayout())
                .with(openOptions)
                .build();
    }

    @Override
    public void addRelationship(InMemoryRelationship rel) throws IOException {
        // Store the relationship
        relLineage.put(rel);

        // Update the in/out edge lists
        var sourceId = rel.getStartNode();
        var targetId = rel.getEndNode();
        // synchronized (outRels) {
        try (var writer = outRels.writer(NULL_CONTEXT)) {
            var key = setKey(
                    eKeys.get().get(0),
                    sourceId,
                    targetId,
                    rel.getStartTimestamp(),
                    rel.getEntityId(),
                    rel.isDeleted());
            writer.put(key, NullValue.INSTANCE);
        }
        // }
        if (sourceId != targetId) {
            // synchronized (inRels) {
            try (var writer = inRels.writer(NULL_CONTEXT)) {
                var key = setKey(
                        eKeys.get().get(0),
                        targetId,
                        sourceId,
                        rel.getStartTimestamp(),
                        rel.getEntityId(),
                        rel.isDeleted());
                writer.put(key, NullValue.INSTANCE);
            }
            // }
        }
    }

    @Override
    public void addRelationships(List<InMemoryRelationship> rels) throws IOException {
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
        var inNeighbourhood =
                (direction == RelationshipDirection.OUTGOING) ? emptyLongList : getInNeighbourhood(nodeId, timestamp);
        var outNeighbourhood =
                (direction == RelationshipDirection.INCOMING) ? emptyLongList : getOutNeighbourhood(nodeId, timestamp);
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
            long nodeId, RelationshipDirection direction, long startTime, long endTime) throws IOException {
        var inNeighbourhoods = (direction == RelationshipDirection.OUTGOING)
                ? emptyNeighbourhoodList
                : getInNeighbourhood(nodeId, startTime, endTime);
        var outNeighbourhoods = (direction == RelationshipDirection.INCOMING)
                ? emptyNeighbourhoodList
                : getOutNeighbourhood(nodeId, startTime, endTime);
        if (inNeighbourhoods.isEmpty() && outNeighbourhoods.isEmpty()) {
            return emptyRelListList;
        }

        // Get and sort all relationships by timestamp
        List<InMemoryNeighbourhood> neighbourhoods = new ArrayList<>();
        if (direction != RelationshipDirection.OUTGOING) {
            neighbourhoods.addAll(inNeighbourhoods);
        }
        if (direction != RelationshipDirection.INCOMING) {
            neighbourhoods.addAll(outNeighbourhoods);
        }
        neighbourhoods.sort(Comparator.comparing(InMemoryNeighbourhood::getStartTimestamp));

        // Go through the relationships and create the final result
        var rels = new ArrayList<List<InMemoryRelationship>>();
        var currentTime = neighbourhoods.get(0).getStartTimestamp();
        var currentRels = relMap.get();
        currentRels.clear();
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

    private void addNeighbourhood(List<InMemoryRelationship> result, List<Long> neighbourhood, long timestamp)
            throws IOException {
        for (var n : neighbourhood) {
            var nextRel = getRelationship(n, timestamp);
            nextRel.ifPresent(result::add);
        }
    }

    @Override
    public List<InMemoryRelationship> getAllRelationships(long timestamp) throws IOException {
        return relLineage.getAll(timestamp);
    }

    @Override
    public void reset() {
        // todo: reset relLineage, inRels, outRels
    }

    private List<Long> getInNeighbourhood(long nodeId, long timestamp) throws IOException {
        return rangeScanByKey(inRels, nodeId, timestamp);
    }

    private List<InMemoryNeighbourhood> getInNeighbourhood(long nodeId, long startTime, long endTime)
            throws IOException {
        return rangeScanByKeyAndTime(inRels, nodeId, startTime, endTime);
    }

    private List<Long> getOutNeighbourhood(long nodeId, long timestamp) throws IOException {
        return rangeScanByKey(outRels, nodeId, timestamp);
    }

    private List<InMemoryNeighbourhood> getOutNeighbourhood(long nodeId, long startTime, long endTime)
            throws IOException {
        return rangeScanByKeyAndTime(outRels, nodeId, startTime, endTime);
    }

    private List<Long> rangeScanByKey(
            GBPTree<MutableQuad<Long, Long, Long, Long>, NullValue> tree, long nodeId, long timestamp)
            throws IOException {
        var fromKey = setKey(eKeys.get().get(0), nodeId, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, false);
        var toKey = setKey(eKeys.get().get(1), nodeId, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, false);

        var relationshipIds = new ArrayList<Long>();
        try (var updates = tree.seek(fromKey, toKey, NULL_CONTEXT)) {
            while (updates.next()) {
                var key = updates.key();
                if (key.third <= timestamp) {
                    var isDeleted = (key.fourth & (0x01)) == 1;
                    var edgeId = key.fourth >> 2;

                    if (isDeleted
                            && !relationshipIds.isEmpty()
                            && relationshipIds.get(relationshipIds.size() - 1) == edgeId) {
                        relationshipIds.remove(relationshipIds.size() - 1);
                    } else if (relationshipIds.isEmpty() || relationshipIds.get(relationshipIds.size() - 1) != edgeId) {
                        relationshipIds.add(edgeId);
                    }
                }
            }
        }
        return relationshipIds;
    }

    private List<InMemoryNeighbourhood> rangeScanByKeyAndTime(
            GBPTree<MutableQuad<Long, Long, Long, Long>, NullValue> tree, long nodeId, long startTime, long endTime)
            throws IOException {
        var fromKey = setKey(eKeys.get().get(0), nodeId, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, false);
        var toKey = setKey(eKeys.get().get(1), nodeId, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, false);

        var result = new ArrayList<InMemoryNeighbourhood>();
        try (var updates = tree.seek(fromKey, toKey, NULL_CONTEXT)) {
            while (updates.next()) {
                var key = updates.key();
                var currentTime = key.third;
                if (currentTime >= startTime && currentTime <= endTime) {
                    var isDeleted = (key.fourth & (0x01)) == 1;
                    var edgeId = key.fourth >> 2;
                    result.add(new InMemoryNeighbourhood(edgeId, currentTime, isDeleted));
                }
            }
        }

        return result;
    }

    public int numberOfRelationships() throws IOException {
        return relLineage.numberOfEntities();
    }

    public int numberOfNeighbourhoodRecords() throws IOException {
        return (int) outRels.estimateNumberOfEntriesInTree(NULL_CONTEXT)
                + (int) inRels.estimateNumberOfEntriesInTree(NULL_CONTEXT);
    }

    @Override
    public void flushIndexes() {
        relLineage.flushIndex();
        outRels.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        inRels.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
    }

    private MutableQuad<Long, Long, Long, Long> setKey(
            MutableQuad<Long, Long, Long, Long> key,
            long leftNode,
            long rightNode,
            long timestamp,
            long edgeId,
            boolean isDeleted) {
        key.setFirst(leftNode);
        key.setSecond(rightNode);
        key.setThird(timestamp);

        // Shift to the left the edgeId and track whether is deleted with the first bit
        edgeId = edgeId << 2;
        edgeId |= isDeleted ? 1 : 0;
        key.setFourth(edgeId);

        return key;
    }

    @Override
    public void shutdown() throws IOException {
        relLineage.shutdown();
        inRels.close();
        outRels.close();
    }

    @Override
    public void setDiffThreshold(int threshold) {
        relLineage.setDiffThreshold(threshold);
    }
}
