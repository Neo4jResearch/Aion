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
package org.neo4j.temporalgraph.timeindex.timestore.disk;

import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.CURRENT_LOG_FORMAT_VERSION;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.MutablePair;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.temporalgraph.entities.ByteEntityUtilities;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.entities.TemporalGraph;
import org.neo4j.temporalgraph.timeindex.SnapshotCreationPolicy;
import org.neo4j.temporalgraph.timeindex.timestore.TimeStore;
import org.neo4j.temporalgraph.utils.IntCircularList;
import org.neo4j.temporalgraph.utils.ThreadPool;
import org.roaringbitmap.RoaringBitmap;

public class PersistentTimeStore implements TimeStore {
    private final SnapshotCreationPolicy policy;
    private final Map<String, Integer> namesToIds;
    private final Map<Integer, String> idsToNames;
    private long updateCounter;

    private final FileSystemAbstraction fs;
    private final FlushableLogPositionAwareChannel logWriter;
    private final LogPositionMarker marker;
    private final Path logPath;
    private final ThreadLocal<StoreChannel> readChannel = new ThreadLocal<>();

    // Index the positions of the log based on timestamp
    // As the GBTree requires unique keys, we store data in the following format:
    // Key: Pair<timestamp, logOffset>, Value: lengthInBytes
    private final GBPTree<MutablePair<Long, Long>, MutableInt> timeIndex;
    private final ThreadLocal<MutablePair<Long, Long>> tempKey1 = ThreadLocal.withInitial(MutablePair::new);
    private final ThreadLocal<MutablePair<Long, Long>> tempKey2 = ThreadLocal.withInitial(MutablePair::new);
    private final ThreadLocal<MutableInt> tempValue = ThreadLocal.withInitial(MutableInt::new);

    // Index the stored snapshots
    private final SnapshotGraphStore graphStore;
    private long currentTimestamp;
    private final InMemoryGraph currentGraph;
    private final boolean storeSnaphots;

    // Thread local variables
    private static final ThreadLocal<IntCircularList> bfsQueue = ThreadLocal.withInitial(IntCircularList::new);
    private final ThreadLocal<RoaringBitmap> visitedNodes = ThreadLocal.withInitial(RoaringBitmap::new);
    private static final ThreadLocal<ByteBuffer> bufferThreadLocal =
            ThreadLocal.withInitial(() -> ByteBuffer.allocate((int) kibiBytes(32)));

    public PersistentTimeStore(
            SnapshotCreationPolicy policy,
            PageCache pageCache,
            FileSystemAbstraction fs,
            Path logPath,
            Path timeIndexPath,
            Map<String, Integer> namesToIds,
            Map<Integer, String> idsToNames)
            throws IOException {
        this.policy = policy;
        this.namesToIds = namesToIds;
        this.idsToNames = idsToNames;
        this.updateCounter = 0;
        this.currentTimestamp = -1L;
        this.logPath = logPath;
        this.fs = fs;

        var channel = fs.write(logPath);
        var versionedChannel = new PhysicalLogVersionedStoreChannel(
                channel, CURRENT_LOG_FORMAT_VERSION, (byte) 0, logPath, EMPTY_ACCESSOR, NULL);
        this.logWriter = new PhysicalFlushableLogPositionAwareChannel(
                versionedChannel, new HeapScopedBuffer(1024, ByteOrder.LITTLE_ENDIAN, INSTANCE));
        this.marker = new LogPositionMarker();

        ImmutableSet<OpenOption> openOptions = Sets.immutable.empty();
        this.timeIndex = new GBPTreeBuilder<>(pageCache, fs, timeIndexPath, new TimeIndexLayout())
                .with(openOptions)
                .build();

        this.graphStore = new SnapshotGraphStore(
                gibiBytes(8), pageCache, fs, logPath.getParent().resolve("SNAPSHOT_INDEX"), namesToIds, idsToNames);

        storeSnaphots = policy.storeSnapshots();
        this.currentGraph = (storeSnaphots) ? InMemoryGraph.createGraph() : null;
    }

    public synchronized void addUpdate(InMemoryEntity entity) throws IOException {
        // Log the update
        var buffer = getAndClearByteBuffer();
        ByteEntityUtilities.serializeEntity(entity, buffer, namesToIds, true);

        // Keep the start offset
        logWriter.getCurrentLogPosition(marker);

        var entityLength = buffer.position();
        logWriter.put(buffer.array(), 0, entityLength);

        // Index its offset. Access does not require synchronization as addUpdate is synchronized.
        try (var writer = timeIndex.writer(NULL_CONTEXT)) {
            var key = setKey(tempKey1, entity.getStartTimestamp(), marker.getByteOffset());
            tempValue.get().setValue(entityLength);
            writer.put(key, tempValue.get());
        }

        // Update the in-memory snapshot
        if (storeSnaphots) {
            currentGraph.updateGraph(entity, false);
        }

        // Increase the current timestamp and updates' counter
        currentTimestamp = Math.max(entity.getStartTimestamp(), currentTimestamp);
        updateCounter++;
    }

    public synchronized void flushLog() throws IOException {
        logWriter.prepareForFlush().flush();
    }

    @Override
    public void flushIndexes() {
        timeIndex.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        graphStore.flushIndex();
    }

    /**
     * When calling this method, we assume that all updates up to @currentTimestamp
     * have been stored. Otherwise, the getGraph() method will not return a correct
     * result.
     */
    public synchronized void takeSnapshot() throws IOException {
        flushLog();
        // check if we have to create a snapshot
        if (policy.readyToTakeSnapshot(updateCounter)) {
            graphStore.storeSnapshot(currentGraph, currentTimestamp);
            updateCounter = 0;
        }
    }

    @Override
    public Optional<InMemoryNode> getNode(long nodeId, long timestamp) throws IOException {
        var graph = getGraphWithFilter(
                timestamp, e -> (e instanceof InMemoryNode node && node.getEntityId() == nodeId), true);
        return graph.getNode((int) nodeId);
    }

    @Override
    public List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) throws IOException {
        FilterInterface filter = e -> (e instanceof InMemoryNode node && node.getEntityId() == nodeId);
        var graph = getGraphWithFilter(startTime, filter, true);
        var node = graph.getNode((int) nodeId);
        InMemoryNode actualNode;

        List<InMemoryNode> result = new ArrayList<>();
        if (node.isPresent()) {
            actualNode = node.get().copy();
            result.add(actualNode);
        } else {
            actualNode = new InMemoryNode(nodeId, startTime);
        }
        getEntityHistory(result, actualNode, filter, startTime + 1, endTime);

        return result;
    }

    @Override
    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) throws IOException {
        var graph = getGraphWithFilter(
                timestamp, e -> (e instanceof InMemoryRelationship node && node.getEntityId() == relId), true);
        return graph.getRelationship((int) relId);
    }

    @Override
    public List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) throws IOException {
        FilterInterface filter = e -> (e instanceof InMemoryRelationship node && node.getEntityId() == relId);
        var graph = getGraphWithFilter(startTime, filter, true);
        var rel = graph.getRelationship((int) relId);
        InMemoryRelationship actualRel;

        List<InMemoryRelationship> result = new ArrayList<>();
        if (rel.isPresent()) {
            actualRel = rel.get().copy();
            result.add(actualRel);
        } else {
            actualRel = new InMemoryRelationship(relId, -1, -1, -1, startTime);
        }
        getEntityHistory(result, actualRel, filter, startTime + 1, endTime);

        return result;
    }

    @Override
    public InMemoryGraph getWindow(long startTime, long endTime) throws IOException {
        // First get a valid graph based on the endTime
        var graph = getGraphWithFilter(endTime, e -> e.getStartTimestamp() <= endTime, false);

        // Then, use the updates between startTime and endTime to track valid nodes
        var diff = getDiff(startTime, endTime);
        MutableIntSet validSet = IntSets.mutable.empty();
        for (var e : diff) {
            if (e instanceof InMemoryRelationship rel) {
                validSet.add((int) rel.getStartNode());
                validSet.add((int) rel.getEndNode());
            } else if (e instanceof InMemoryNode node) {
                validSet.add((int) node.getEntityId());
            }
        }

        // Based on the valid nodes from before, find the remaining reachable nodes
        var iter = validSet.intIterator();
        var queue = new IntCircularList();
        while (iter.hasNext()) {
            var nodeId = iter.next();
            queue.add(nodeId);
        }

        var finalSet = IntSets.mutable.empty();
        while (!queue.isEmpty()) {
            var nodeId = queue.poll();
            finalSet.add(nodeId);
            for (var neighbour : graph.getRelationships(nodeId, RelationshipDirection.OUTGOING)) {
                var neighbourId = (int) neighbour.getEntityId();
                if (!finalSet.contains(neighbourId)) {
                    queue.add(neighbourId);
                }
            }
        }

        // Using all the reachable nodes, filter the initial snapshot
        var nodeIterator = graph.getNodeMap().iterator();
        while (nodeIterator.hasNext()) {
            if (!finalSet.contains(nodeIterator.getIndex())) {
                nodeIterator.delete();
            } else {
                nodeIterator.next();
            }
        }

        var relIterator = graph.getRelationshipMap().iterator();
        while (relIterator.hasNext()) {
            var rel = relIterator.peek();
            if (!finalSet.contains((int) rel.getStartNode()) || !finalSet.contains((int) rel.getEndNode())) {
                relIterator.delete();
            } else {
                relIterator.next();
            }
        }
        graph.rebuildEdgeLists();

        return graph;
    }

    @Override
    public List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp)
            throws IOException {
        var graph = getGraphWithFilter(
                timestamp,
                e -> {
                    if (e instanceof InMemoryNode node && node.getEntityId() == nodeId) {
                        return true;
                    } else
                        return e instanceof InMemoryRelationship rel
                                && ((rel.getStartNode() == nodeId
                                                && (direction == RelationshipDirection.BOTH
                                                        || direction == RelationshipDirection.OUTGOING))
                                        || (rel.getEndNode() == nodeId
                                                && (direction == RelationshipDirection.BOTH
                                                        || direction == RelationshipDirection.INCOMING)));
                },
                true);

        return graph.getRelationships((int) nodeId, direction);
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) throws IOException {
        var result = new ArrayList<List<InMemoryRelationship>>();

        FilterInterface filter = e -> {
            if (e instanceof InMemoryNode node && node.getEntityId() == nodeId) {
                return true;
            } else
                return e instanceof InMemoryRelationship rel
                        && ((rel.getStartNode() == nodeId
                                        && (direction == RelationshipDirection.BOTH
                                                || direction == RelationshipDirection.OUTGOING))
                                || (rel.getEndNode() == nodeId
                                        && (direction == RelationshipDirection.BOTH
                                                || direction == RelationshipDirection.INCOMING)));
        };

        var currentTime = startTime;
        var graph = getGraphWithFilter(startTime, filter, true);

        var lowKey = setKey(tempKey1, startTime, Long.MIN_VALUE);
        var highKey = setKey(tempKey2, endTime + 1, Long.MAX_VALUE);
        try (var updates = timeIndex.seek(lowKey, highKey, NULL_CONTEXT)) {
            var channel = getReadChannel();
            ByteBuffer buffer = null;
            while (updates.next()) {
                var offset = updates.key().getRight();
                var length = updates.value().intValue();

                if (buffer == null || buffer.remaining() < length) {
                    buffer = readFromChannel(channel, offset);
                }
                var update = ByteEntityUtilities.deserializeEntity(buffer, idsToNames);
                if (filter.apply(update)) {
                    if (currentTime != update.getStartTimestamp()) {
                        result.add(graph.getRelationships((int) nodeId, direction));
                        currentTime = update.getStartTimestamp();
                    }
                    graph.updateGraph(update, true);
                }
            }
        }
        result.add(graph.getRelationships((int) nodeId, direction));

        return result;
    }

    @Override
    public List<InMemoryNode> expand(long nodeId, RelationshipDirection direction, int hops, long timestamp)
            throws IOException {
        if (direction != RelationshipDirection.OUTGOING) {
            throw new UnsupportedOperationException("Supporting only outgoing edges now");
        }

        var result = new ArrayList<InMemoryNode>();

        var graph = getGraph(timestamp);

        var bitmap = visitedNodes.get();
        var queue = bfsQueue.get();
        queue.clear();

        queue.add((int) nodeId);
        for (int i = 0; i < hops; i++) {

            bitmap.clear();
            int queueSize = queue.size();
            for (int j = 0; j < queueSize; ++j) {
                var currentNode = queue.poll();
                var rels = graph.getRelationships(currentNode, direction);
                for (var r : rels) {
                    // Outgoing edge
                    if (r.getStartNode() == currentNode) {

                        // Get target node
                        var targetId = (int) r.getEndNode();
                        var targetNode = graph.getNode(targetId);
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
    public InMemoryGraph getGraph(long timestamp) throws IOException {
        return getGraphWithFilter(timestamp, e -> e.getStartTimestamp() <= timestamp, true);
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
    public List<InMemoryEntity> getDiff(long startTime, long endTime) throws IOException {
        List<InMemoryEntity> result = new ArrayList<>();
        var lowKey = setKey(tempKey1, startTime, Long.MIN_VALUE);
        var highKey = setKey(tempKey2, endTime + 1, Long.MAX_VALUE);
        try (var updates = timeIndex.seek(lowKey, highKey, NULL_CONTEXT)) {
            var channel = getReadChannel();
            ByteBuffer buffer = null;
            while (updates.next()) {
                var offset = updates.key().getRight();
                var length = updates.value().intValue();

                if (buffer == null || buffer.remaining() < length) {
                    buffer = readFromChannel(channel, offset);
                }
                var update = ByteEntityUtilities.deserializeEntity(buffer, idsToNames);
                result.add(update);
            }
        }
        return result;
    }

    @Override
    public TemporalGraph getTemporalGraph(long startTime, long endTime) throws IOException {
        if (startTime > endTime) {
            return new TemporalGraph();
        }

        // Gather base snapshot
        InMemoryGraph baseSnapshot = getGraphWithFilter(startTime, e -> e.getStartTimestamp() <= startTime, false);
        var graph = new TemporalGraph();
        graph.initialize(baseSnapshot);

        // Project diffs atop temporal graph
        var diff = getDiff(startTime + 1, endTime);
        for (var e : diff) {
            if (e instanceof InMemoryRelationship rel) {
                if (!rel.isDeleted()) {
                    graph.addRelationship(rel);
                } else {
                    graph.deleteRelationship((int) rel.getEntityId(), rel.getStartTimestamp());
                }
            } else if (e instanceof InMemoryNode node) {
                if (!node.isDeleted()) {
                    graph.addNode(node);
                } else {
                    graph.deleteNode((int) node.getEntityId(), node.getStartTimestamp());
                }
            }
        }
        // Update end time
        graph.setEndTime(endTime);

        return graph;
    }

    public void reset() {
        updateCounter = 0;
        currentTimestamp = -1L;
        graphStore.clear();
        if (currentGraph != null) {
            currentGraph.clear();
        }
        // todo: clear log and timeIndex
    }

    private InMemoryGraph getGraphWithFilter(long timestamp, FilterInterface filter, boolean updateNeighbourhood)
            throws IOException {
        // First try to get the most recent snapshot
        var result = graphStore.getSnapshot(timestamp);
        var prevTimestamp = result.getLeft();
        var graph = result.getRight();

        // Now mutate the graph with any missing updates
        // todo: allow backwards updates
        // Perform updates in a forward fashion
        if (prevTimestamp < timestamp) {

            // Create a copy of the graph to maintain the stored snapshot.
            // The copy re-creates the in/out relationships.
            if (graph.getNodeMap().size() > 0) {
                // graph = graph.copy();
                // graph = graph.parallelCopy();
                graph = graph.parallelCopyWithoutObjectCreation(updateNeighbourhood);
            }

            synchronousUpdate(filter, prevTimestamp, timestamp, graph, updateNeighbourhood);
        } else {
            graph.rebuildEdgeLists();
        }
        return graph;
    }

    private void synchronousUpdate(
            FilterInterface filter,
            long prevTimestamp,
            long timestamp,
            InMemoryGraph graph,
            boolean updateNeighbourhood)
            throws IOException {
        var lowKey = setKey(tempKey1, prevTimestamp, Long.MIN_VALUE);
        var highKey = setKey(tempKey2, timestamp + 1, Long.MAX_VALUE);
        try (var updates = timeIndex.seek(lowKey, highKey, NULL_CONTEXT)) {
            var channel = getReadChannel();
            ByteBuffer buffer = null;
            while (updates.next()) {
                var offset = updates.key().getRight();
                var length = updates.value().intValue();

                if (buffer == null || buffer.remaining() < length) {
                    buffer = readFromChannel(channel, offset);
                }
                var update = ByteEntityUtilities.deserializeEntity(buffer, idsToNames);
                if (filter.apply(update)) {
                    graph.updateGraph(update, updateNeighbourhood);
                }
            }
        }
    }

    private void asynchronousUpdate(FilterInterface filter, long prevTimestamp, long timestamp, InMemoryGraph graph)
            throws IOException {
        var lowKey = setKey(tempKey1, prevTimestamp, Long.MIN_VALUE);
        var highKey = setKey(tempKey2, timestamp + 1, Long.MAX_VALUE);
        try (var updates = timeIndex.seek(lowKey, highKey, NULL_CONTEXT)) {

            List<InMemoryEntity> entities = new ArrayList<>();
            var channel = getReadChannel();
            final int batchSize = 512;
            Future<Boolean> future = null;
            ByteBuffer buffer = null;
            try {
                while (updates.next()) {
                    var offset = updates.key().getRight();
                    var length = updates.value().intValue();

                    if (buffer == null || buffer.remaining() < length) {
                        buffer = readFromChannel(channel, offset);
                    }
                    var update = ByteEntityUtilities.deserializeEntity(buffer, idsToNames);
                    if (filter.apply(update)) {
                        entities.add(update);
                    }

                    if (entities.size() == batchSize) {
                        if (future != null) {
                            future.get();
                        }
                        future = processEntities(entities, graph);
                        entities = new ArrayList<>();
                    }
                }

                if (future != null) {
                    future.get();
                }
                if (!entities.isEmpty()) {
                    future = processEntities(entities, graph);
                    future.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Future<Boolean> processEntities(final List<InMemoryEntity> entities, final InMemoryGraph graph) {
        return ThreadPool.getInstance().submit(() -> {
            for (var e : entities) {
                graph.updateGraph(e, true);
            }
            return true;
        });
    }

    private ByteBuffer getAndClearByteBuffer() {
        var buffer = bufferThreadLocal.get();
        buffer.clear();
        return buffer;
    }

    private ByteBuffer readFromChannel(StoreChannel readChannel, long offset, int length) throws IOException {
        readChannel.position(offset);

        var buffer = getAndClearByteBuffer();
        buffer.limit(length);
        readChannel.read(buffer);

        buffer.flip();
        return buffer;
    }

    private ByteBuffer readFromChannel(StoreChannel readChannel, long offset) throws IOException {
        readChannel.position(offset);

        var buffer = getAndClearByteBuffer();
        readChannel.read(buffer);

        buffer.flip();
        return buffer;
    }

    private <T extends InMemoryEntity> void getEntityHistory(
            List<T> result, T entity, FilterInterface filter, long startTime, long endTime) throws IOException {
        var lowKey = setKey(tempKey1, startTime, Long.MIN_VALUE);
        var highKey = setKey(tempKey2, endTime + 1, Long.MAX_VALUE);
        try (var updates = timeIndex.seek(lowKey, highKey, NULL_CONTEXT)) {
            var channel = getReadChannel();
            ByteBuffer buffer = null;
            while (updates.next()) {
                var offset = updates.key().getRight();
                var length = updates.value().intValue();

                if (buffer == null || buffer.remaining() < length) {
                    buffer = readFromChannel(channel, offset);
                }
                var update = ByteEntityUtilities.deserializeEntity(buffer, idsToNames);
                if (filter.apply(update)) {
                    entity = (T) entity.copy();
                    entity.merge(update);
                    result.add(entity);
                }
            }
        }
    }

    private MutablePair<Long, Long> setKey(ThreadLocal<MutablePair<Long, Long>> key, long left, long right) {
        key.get().setLeft(left);
        key.get().setRight(right);
        return key.get();
    }

    @Override
    public void shutdown() throws IOException {
        logWriter.close();
        timeIndex.close();
        graphStore.close();
    }

    private StoreChannel getReadChannel() throws IOException {
        if (readChannel.get() == null) {
            readChannel.set(fs.read(logPath));
        }
        return readChannel.get();
    }
}
