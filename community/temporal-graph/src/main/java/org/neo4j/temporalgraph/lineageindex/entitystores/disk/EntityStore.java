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
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.MutablePair;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.factory.Lists;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.temporalgraph.entities.ByteEntityUtilities;
import org.neo4j.temporalgraph.entities.InMemoryEntity;

public class EntityStore<T extends InMemoryEntity> {

    private final Map<String, Integer> namesToIds;
    private final Map<Integer, String> idsToNames;

    // Key: Pair<id, timestamp>, Value: RawBytes
    private final GBPTree<MutablePair<Long, Long>, BufferedBytes> tree;
    private final ThreadLocal<List<MutablePair<Long, Long>>> lKeys =
            ThreadLocal.withInitial(() -> Arrays.asList(new MutablePair<>(), new MutablePair<>()));
    private final ThreadLocal<BufferedBytes> tempBuffer = ThreadLocal.withInitial(() -> new BufferedBytes(512));
    // todo: replace List<T> with a circular buffer of pre-allocated values
    private final ThreadLocal<List<T>> tempValues = ThreadLocal.withInitial(Lists.mutable::empty);

    // Variables used for materializing diffs
    private int DIFF_THRESHOLD = -1;
    private final Map<Long, Integer> diffMap = new ConcurrentHashMap<>();

    public EntityStore(
            PageCache pageCache,
            FileSystemAbstraction fs,
            Path path,
            Map<String, Integer> namesToIds,
            Map<Integer, String> idsToNames) {
        this.namesToIds = namesToIds;
        this.idsToNames = idsToNames;

        ImmutableSet<OpenOption> openOptions = Sets.immutable.empty();
        tree = new GBPTreeBuilder<>(pageCache, fs, path, new LineageIndexLayout())
                .with(openOptions)
                .build();
    }

    // Synchronization is required for calling the prepare() method (i.e., materializing deltas)
    public void put(T entity) throws IOException {
        entity = prepare(entity);

        try (var writer = tree.writer(NULL_CONTEXT)) {
            putEntity(entity.getEntityId(), entity, writer);
        }
    }

    public void put(long keyId, T entity) throws IOException {
        try (var writer = tree.writer(NULL_CONTEXT)) {
            putEntity(keyId, entity, writer);
        }
    }

    public void put(List<T> entities) throws IOException {
        prepare(entities);

        try (var writer = tree.writer(NULL_CONTEXT)) {
            for (var entity : entities) {
                putEntity(entity.getEntityId(), entity, writer);
            }
        }
    }

    private void putEntity(long keyId, T entity, Writer<MutablePair<Long, Long>, BufferedBytes> writer) {
        var key = setKey(lKeys.get().get(0), keyId, entity.getStartTimestamp());

        var buffer = getAndClearByteBuffer();
        ByteEntityUtilities.serializeEntity(entity, buffer, namesToIds, false);
        buffer.flip();

        tempBuffer.get().setLimit(buffer.limit());
        var value = tempBuffer.get();
        writer.put(key, value);
    }

    public Optional<T> get(long entityId, long timestamp) throws IOException {
        // First find the minimum range of values required to reconstruct the entity.
        // We search backwards using the given timestamp.
        var lowTimestamp = Long.MIN_VALUE;
        var values = tempValues.get();
        values.clear();

        var lowKey = setKey(lKeys.get().get(0), entityId, lowTimestamp);
        var highKey = setKey(lKeys.get().get(1), entityId, timestamp);
        try (var updates = tree.seek(highKey, lowKey, NULL_CONTEXT)) {
            while (updates.next() && !ByteEntityUtilities.entityIsDeleted(updates.value().buffer)) {
                var serializedEntity = updates.value().buffer;
                lowTimestamp = updates.key().right;
                values.add((T) ByteEntityUtilities.deserializeEntity(
                        serializedEntity, updates.key().left, updates.key().right, idsToNames));
                serializedEntity.position(0);
                if (!ByteEntityUtilities.entityIsDiff(serializedEntity)) {
                    break;
                }
            }
        }

        // If none is found, return an empty optional
        if (lowTimestamp == Long.MIN_VALUE) {
            return Optional.empty();
        }

        // Otherwise, we have bounded the range from lowTimestamp to timestamp
        // and perform a forward search
        var result = values.get(values.size() - 1);
        for (int i = values.size() - 2; i >= 0; --i) {
            result.merge(values.get(i));
        }
        return Optional.of(result);
    }

    public List<T> get(long entityId, long startTime, long endTime) throws IOException {
        var result = new ArrayList<T>();

        var lowKey = setKey(lKeys.get().get(0), entityId, startTime);
        var highKey = setKey(lKeys.get().get(1), entityId, endTime + 1); // the highKey is exclusive
        T currentEntity = null;
        try (var updates = tree.seek(lowKey, highKey, NULL_CONTEXT)) {
            boolean wasDeleted = false;
            while (updates.next()) {
                var entity = (T) ByteEntityUtilities.deserializeEntity(
                        updates.value().buffer, updates.key().left, updates.key().right, idsToNames);

                if (currentEntity != null) {
                    currentEntity.merge(entity);
                } else {
                    currentEntity = entity;
                }

                if (!wasDeleted) {
                    updateLastElement(result, currentEntity.getStartTimestamp());
                }
                if (!currentEntity.isDeleted()) {
                    result.add(currentEntity);
                    currentEntity = (T) currentEntity.copy();
                    wasDeleted = false;
                } else {
                    currentEntity = null;
                    wasDeleted = true;
                }
            }
        }
        return result;
    }

    public List<T> getAll(long timestamp) throws IOException {
        var result = new ArrayList<T>();
        var lowKey = setKey(lKeys.get().get(0), Long.MIN_VALUE, Long.MIN_VALUE);
        var highKey = setKey(lKeys.get().get(1), Long.MAX_VALUE, Long.MAX_VALUE); // the highKey is exclusive
        try (var updates = tree.seek(lowKey, highKey, NULL_CONTEXT)) {
            T currentEntity = null;
            long prevEntityId = -1;
            while (updates.next()) {
                var currentTime = updates.key().right;
                if (currentTime > timestamp) {
                    continue;
                }

                var entity = (T) ByteEntityUtilities.deserializeEntity(
                        updates.value().buffer, updates.key().left, updates.key().right, idsToNames);
                if (entity.getEntityId() != prevEntityId || currentEntity == null) {
                    if (currentEntity != null && !currentEntity.isDeleted()) {
                        result.add(currentEntity);
                    }
                    currentEntity = entity;
                } else {
                    currentEntity.merge(entity);
                }
                prevEntityId = currentEntity.getEntityId();
            }
            if (currentEntity != null && !currentEntity.isDeleted()) {
                result.add(currentEntity);
            }
        }
        return result;
    }

    public int numberOfEntities() throws IOException {
        return (int) tree.estimateNumberOfEntriesInTree(NULL_CONTEXT);
    }

    private T prepare(T entity) throws IOException {
        if (DIFF_THRESHOLD != -1) {
            return synchronizedPrepare(entity);
        }
        return entity;
    }

    private synchronized T synchronizedPrepare(T entity) throws IOException {
        var id = entity.getEntityId();
        if (entity.isDiff()) {
            var prevCount = diffMap.getOrDefault(id, 0);
            var newCount = prevCount + 1;
            if (newCount >= DIFF_THRESHOLD) {
                // get previous version and merge diff
                var prev = get(id, entity.getStartTimestamp());
                if (prev.isPresent()) {
                    prev.get().merge(entity);
                    entity = prev.get();
                }
                entity.unsetDiff();

                diffMap.put(id, 0);
            } else {
                diffMap.put(id, newCount);
            }

        } else {
            // Both deleted and materialzed entities do not count as diffs
            diffMap.put(id, 0);
        }
        return entity;
    }

    private void prepare(List<T> entities) throws IOException {
        if (DIFF_THRESHOLD != -1) {
            for (int i = 0; i < entities.size(); ++i) {
                var entity = entities.get(i);
                entities.set(i, prepare(entity));
            }
        }
    }

    private boolean entityIdExists(long entityId) throws IOException {
        var lowKey = setKey(lKeys.get().get(0), entityId, Long.MIN_VALUE);
        var highKey = setKey(lKeys.get().get(1), entityId, Long.MAX_VALUE);
        try (var updates = tree.seek(lowKey, highKey, NULL_CONTEXT)) {
            return updates.next();
        }
    }

    private MutablePair<Long, Long> setKey(MutablePair<Long, Long> key, long left, long right) {
        key.setLeft(left);
        key.setRight(right);
        return key;
    }

    private ByteBuffer getAndClearByteBuffer() {
        var buffer = tempBuffer.get().buffer;
        buffer.clear();
        return buffer;
    }

    public void flushIndex() {
        tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
    }

    public void shutdown() throws IOException {
        tree.close();
    }

    public void setDiffThreshold(int threshold) {
        DIFF_THRESHOLD = threshold;
    }

    private void updateLastElement(List<T> result, long timestamp) {
        if (!result.isEmpty()) {
            result.get(result.size() - 1).setEndTimestamp(timestamp);
        }
    }
}
