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
package org.neo4j.temporalgraph.benchmarks;

import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.config;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.PersistentLineageStore;
import org.neo4j.temporalgraph.timeindex.SnapshotCreationPolicy;
import org.neo4j.temporalgraph.timeindex.timestore.disk.PersistentTimeStore;

public class TemporalStore {
    private final Path dbPath;
    private final Map<String, Integer> namesToIds;
    private final Map<Integer, String> idsToNames;
    private final PersistentLineageStore lineageStore;
    private final PersistentTimeStore timeStore;
    private long counter;
    private final long numberOfChanges;
    private long lastTimestamp;

    public TemporalStore(long numberOfChanges) throws IOException {
        dbPath = Paths.get("").toAbsolutePath().resolve("target/neo4j-store-benchmark/data");
        Files.createDirectories(dbPath);

        namesToIds = new HashMap<>();
        idsToNames = new HashMap<>();
        // Populate maps
        for (int i = 0; i < 32; ++i) {
            namesToIds.put("Property" + i, i);
            idsToNames.put(i, "Property" + i);
        }

        counter = 0;
        this.numberOfChanges = numberOfChanges;
        lastTimestamp = 0;

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        final int pageSize = (int) kibiBytes(16);
        var memoryAllocator = MemoryAllocator.createAllocator(gibiBytes(32), EmptyMemoryTracker.INSTANCE);
        MuninnPageCache.Configuration configuration =
                config(memoryAllocator).pageCacheTracer(PageCacheTracer.NULL).pageSize(pageSize);
        var pageCache = StandalonePageCacheFactory.createPageCache(
                fs, createInitialisedScheduler(), PageCacheTracer.NULL, configuration);

        var nodeStorePath = dbPath.toAbsolutePath().resolve("NODE_STORE_INDEX");
        var relStorePath = dbPath.toAbsolutePath().resolve("REL_STORE_INDEX");
        lineageStore = new PersistentLineageStore(pageCache, fs, nodeStorePath, relStorePath, namesToIds, idsToNames);

        var policy = new SnapshotCreationPolicy(numberOfChanges);
        var dataLogPath = dbPath.toAbsolutePath().resolve("DATA_LOG");
        var timeIndexPath = dbPath.toAbsolutePath().resolve("TIME_INDEX");
        timeStore = new PersistentTimeStore(policy, pageCache, fs, dataLogPath, timeIndexPath, namesToIds, idsToNames);
    }

    public TemporalStore(
            int numberOfChanges, Path dbPath, PersistentLineageStore lineageStore, PersistentTimeStore timeStore) {
        this.numberOfChanges = numberOfChanges;
        this.dbPath = dbPath;
        this.lineageStore = lineageStore;
        this.timeStore = timeStore;
        this.namesToIds = null;
        this.idsToNames = null;
    }

    public void addNodes(List<InMemoryNode> nodes) throws IOException {
        for (var n : nodes) {
            timeStore.addUpdate(n);
            tryToCreateSnapshot();
            updateTimestamp(n.getStartTimestamp());
        }
        timeStore.flushLog();

        lineageStore.addNodes(nodes);
    }

    public void addRelationships(List<InMemoryRelationship> relationships) throws IOException {
        for (var r : relationships) {
            timeStore.addUpdate(r);
            tryToCreateSnapshot();
            updateTimestamp(r.getStartTimestamp());
        }
        timeStore.flushLog();

        lineageStore.addRelationships(relationships);
    }

    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) throws IOException {
        return lineageStore.getRelationship(relId, timestamp);
    }

    public List<InMemoryNode> expand(long nodeId, int hops, long timestamp, boolean useTimeStore) throws IOException {
        return (useTimeStore)
                ? timeStore.expand(nodeId, RelationshipDirection.OUTGOING, hops, timestamp)
                : lineageStore.expand(nodeId, RelationshipDirection.OUTGOING, hops, timestamp);
    }

    public InMemoryGraph getSnapshot(long timestamp) throws IOException {
        return timeStore.getGraph(timestamp);
    }

    public List<InMemoryEntity> getDiff(long startTime, long endTime) throws IOException {
        return timeStore.getDiff(startTime, endTime);
    }

    void tryToCreateSnapshot() throws IOException {
        counter++;
        if (counter == numberOfChanges) {
            timeStore.takeSnapshot();
            counter = 0;
        }
    }

    public void flush() {
        timeStore.flushIndexes();
        lineageStore.flushIndexes();
    }

    public void clear() throws IOException {
        timeStore.shutdown();
        lineageStore.shutdown();

        var nodeStorePath = dbPath.toAbsolutePath().resolve("NODE_STORE_INDEX");
        Files.deleteIfExists(nodeStorePath);

        var relStorePath = dbPath.toAbsolutePath().resolve("REL_STORE_INDEX");
        Files.deleteIfExists(relStorePath);

        var dataLogPath = dbPath.toAbsolutePath().resolve("DATA_LOG");
        Files.deleteIfExists(dataLogPath);

        var timeIndexPath = dbPath.toAbsolutePath().resolve("TIME_INDEX");
        Files.deleteIfExists(timeIndexPath);

        FileUtils.deleteDirectory(dbPath);
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setDiffThreshold(int threshold) {
        lineageStore.setDiffThreshold(threshold);
    }

    private void updateTimestamp(long timestamp) {
        lastTimestamp = Math.max(lastTimestamp, timestamp);
    }
}
