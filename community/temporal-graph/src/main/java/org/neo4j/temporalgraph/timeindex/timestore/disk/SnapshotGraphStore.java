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

import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.CURRENT_LOG_FORMAT_VERSION;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.temporalgraph.entities.ByteEntityUtilities;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.lineageindex.entitystores.disk.NullValue;

public class SnapshotGraphStore {
    private final FileSystemAbstraction fs;
    private final Path snapshotPath;
    private final long memoryBudget;
    private final Map<String, Integer> namesToIds;
    private final Map<Integer, String> idsToNames;
    private final Map<Long, InMemoryGraph> cache;

    private long currentBudget;

    private final GBPTree<MutableLong, NullValue> snapshotIndex;

    private static final int HSB_SIZE = (int) mebiBytes(16);
    ThreadLocal<HeapScopedBuffer> heapScopedBuffer =
            ThreadLocal.withInitial(() -> new HeapScopedBuffer(HSB_SIZE, ByteOrder.LITTLE_ENDIAN, INSTANCE));
    private static final String SNAPSHOT_PREFIX = "snapshot-";

    public SnapshotGraphStore(
            long memoryBudget,
            PageCache pageCache,
            FileSystemAbstraction fs,
            Path snapshotPath,
            Map<String, Integer> namesToIds,
            Map<Integer, String> idsToNames) {
        this.memoryBudget = memoryBudget;
        this.fs = fs;
        this.snapshotPath = snapshotPath;
        this.idsToNames = idsToNames;
        this.namesToIds = namesToIds;
        this.cache = new ConcurrentHashMap<>();
        this.currentBudget = 0L;

        ImmutableSet<OpenOption> openOptions = Sets.immutable.empty();
        this.snapshotIndex = new GBPTreeBuilder<>(pageCache, fs, snapshotPath, new LongLayout())
                .with(openOptions)
                .build();
    }

    public synchronized void storeSnapshot(InMemoryGraph graph, long timestamp) throws IOException {
        // Update the in-memory cache
        updateCache(graph, timestamp);

        // Store the graph to disk
        serializeGraphToDisk(graph, timestamp);

        // Store the timestamp in the btree
        try (var writer = snapshotIndex.writer(NULL_CONTEXT)) {
            var key = new MutableLong(timestamp);
            writer.put(key, NullValue.INSTANCE);
        }
    }

    public Pair<Long, InMemoryGraph> getSnapshot(long timestamp) throws IOException {
        var lastTimestamp = -1L;
        var lowKey = new MutableLong(Long.MIN_VALUE);
        var highKey = new MutableLong(timestamp + 1);
        try (var updates = snapshotIndex.seek(highKey, lowKey, NULL_CONTEXT)) {
            if (updates.next()) {
                lastTimestamp = updates.key().longValue();
            }
        }

        InMemoryGraph graph;
        if (lastTimestamp != -1) {
            // First go ot the cache
            graph = cache.get(lastTimestamp);
            if (graph == null) {
                graph = getGraphFromDisk(lastTimestamp);
                updateCache(graph, lastTimestamp);
            }

        } else {
            graph = InMemoryGraph.createGraph();
        }

        return ImmutablePair.of(lastTimestamp, graph);
    }

    public void flushIndex() {
        snapshotIndex.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
    }

    public void clear() {
        this.currentBudget = 0;
        this.cache.clear();
        // todo: delete snapshots from disk and clear snapshotIndex
    }

    public void close() throws IOException {
        this.snapshotIndex.close();
    }

    private void serializeGraphToDisk(InMemoryGraph graph, long timestamp) throws IOException {
        var path = snapshotPath.getParent().resolve(SNAPSHOT_PREFIX + timestamp);
        var channel = fs.write(path);
        var versionedChannel = new PhysicalLogVersionedStoreChannel(
                channel, CURRENT_LOG_FORMAT_VERSION, (byte) 0, path, EMPTY_ACCESSOR, NULL);
        var logWriter = new PhysicalFlushableLogPositionAwareChannel(versionedChannel, heapScopedBuffer.get());

        // Use the underlying ByteBuffer directly
        var buffer = heapScopedBuffer.get().getBuffer();
        buffer.clear();

        // First store the number of nodes/relationships
        var nodes = graph.getNodeMap();
        var relationships = graph.getRelationshipMap();
        buffer.putLong(nodes.size());
        buffer.putLong(relationships.size());

        // Then store the actual entities
        var nodeIterator = nodes.iterator();
        while (nodeIterator.hasNext()) {
            var n = nodeIterator.next();
            if (buffer.position() + n.getSize() >= buffer.capacity()) {
                logWriter.prepareForFlush().flush();
            }

            ByteEntityUtilities.serializeEntity(n, buffer, namesToIds);
        }

        var relIterator = relationships.iterator();
        while (relIterator.hasNext()) {
            var r = relIterator.next();
            if (buffer.position() + r.getSize() >= buffer.capacity()) {
                logWriter.prepareForFlush().flush();
            }

            ByteEntityUtilities.serializeEntity(r, buffer, namesToIds);
        }

        if (buffer.position() != 0) {
            logWriter.prepareForFlush().flush();
        }
        // cannot call logWriter.close() because it invalidates the thread local ByteBuffer
        versionedChannel.close();
    }

    private InMemoryGraph getGraphFromDisk(long timestamp) throws IOException {
        var path = snapshotPath.getParent().resolve(SNAPSHOT_PREFIX + timestamp);
        try (var file = new RandomAccessFile(path.toFile(), "r")) {
            // Get file channel in read-only mode
            var fileChannel = file.getChannel();

            // Get direct byte buffer access using channel.map() operation
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            // Move the position after the node and relationship counters
            buffer.position(Long.BYTES + Long.BYTES);

            var graph = InMemoryGraph.createGraph();
            while (buffer.hasRemaining()) {
                var entity = ByteEntityUtilities.deserializeEntity(buffer, idsToNames);
                graph.updateGraph(entity, true);
            }

            return graph;
        }
    }

    private void updateCache(InMemoryGraph graph, long timestamp) {
        var graphSize = graph.getSize();
        currentBudget += graphSize;
        if (currentBudget >= memoryBudget) {
            // todo: implement LRU logic for cache
            var iterator = cache.entrySet().iterator();
            while (iterator.hasNext() && currentBudget >= memoryBudget) {
                currentBudget -= iterator.next().getValue().getSize();
                iterator.remove();
            }
            // If there is still not enough space return
            if (currentBudget >= memoryBudget) {
                currentBudget -= graphSize;
                return;
            }
        }
        cache.put(timestamp, graph.copyWithoutNeighbourhood());
    }
}
