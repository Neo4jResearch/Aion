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
package org.neo4j.temporalgraph.timeindex.timestore;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.temporalgraph.HistoryStore;
import org.neo4j.temporalgraph.HistoryTracker;
import org.neo4j.temporalgraph.timeindex.SnapshotCreationPolicy;
import org.neo4j.temporalgraph.timeindex.timestore.disk.PersistentTimeStore;
import org.neo4j.temporalgraph.timeindex.timestore.memory.InMemoryTimeStore;

public class TimeBasedTracker extends HistoryTracker {
    private final TimeStore timeStore;

    public TimeBasedTracker(SnapshotCreationPolicy policy) throws IOException {
        this(policy, null, null, null, null);
    }

    public TimeBasedTracker(
            SnapshotCreationPolicy policy,
            PageCache pageCache,
            FileSystemAbstraction fs,
            Path logPath,
            Path timeIndexPath)
            throws IOException {
        super();
        timeStore = (pageCache == null || fs == null || logPath == null || timeIndexPath == null)
                ? new InMemoryTimeStore(policy)
                : new PersistentTimeStore(policy, pageCache, fs, logPath, timeIndexPath, namesToIds, idsToNames);
    }

    @Override
    public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
        // Update the last transaction time and id
        lastCommittedTime = getTimestamp(data);
        lastTransactionId = getTransactionId(data);
        try {
            processUpdates(data);
            // Flush log to disk after adding all updates
            timeStore.flushLog();
        } catch (IOException e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
        super.afterCommit(data, state, databaseService);
    }

    public HistoryStore getTimeStore() {
        return timeStore;
    }

    @Override
    public void reset() {
        super.reset();
        timeStore.reset();
    }

    private void processUpdates(TransactionData data) throws IOException {
        var nodes = getNodes(data, getTimestamp(data));
        for (var n : nodes) {
            timeStore.addUpdate(n);
        }

        var rels = getRelationships(data, getTimestamp(data));
        for (var r : rels) {
            timeStore.addUpdate(r);
        }

        // Try to create an update if required
        timeStore.takeSnapshot();
    }

    public void shutdown() throws IOException {
        timeStore.shutdown();
    }
}
