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
package org.neo4j.temporalgraph.lineageindex;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.temporalgraph.HistoryTracker;

public class EntityLineageTracker extends HistoryTracker {
    private final LineageStore lineageStore;

    public EntityLineageTracker() {
        this(null, null, null, null);
    }

    public EntityLineageTracker(PageCache pageCache, FileSystemAbstraction fs, Path nodeIndexPath, Path relIndexPath) {
        super();
        lineageStore = (pageCache == null || fs == null || nodeIndexPath == null || relIndexPath == null)
                ? new InMemoryLineageStore()
                : new PersistentLineageStore(pageCache, fs, nodeIndexPath, relIndexPath, namesToIds, idsToNames);
    }

    @Override
    public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
        // Update the last transaction time and id
        lastCommittedTime = getTimestamp(data);
        lastTransactionId = getTransactionId(data);
        try {
            processUpdates(data);
        } catch (IOException e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
        super.afterCommit(data, state, databaseService);
    }

    public LineageStore getLineageStore() {
        return lineageStore;
    }

    @Override
    public void reset() {
        super.reset();
        lineageStore.reset();
    }

    private void processUpdates(TransactionData data) throws IOException {
        processNodeUpdates(data);
        processRelationshipUpdates(data);
    }

    private void processNodeUpdates(TransactionData data) throws IOException {
        if (data.createdNodes().iterator().hasNext()
                || data.assignedNodeProperties().iterator().hasNext()
                || data.assignedLabels().iterator().hasNext()
                || data.deletedNodes().iterator().hasNext()
                || data.removedNodeProperties().iterator().hasNext()
                || data.removedLabels().iterator().hasNext()) {
            var nodes = getNodes(data, getTimestamp(data));
            lineageStore.addNodes(nodes);
        }
    }

    private void processRelationshipUpdates(TransactionData data) throws IOException {
        if (data.createdRelationships().iterator().hasNext()
                || data.assignedRelationshipProperties().iterator().hasNext()
                || data.deletedRelationships().iterator().hasNext()
                || data.removedRelationshipProperties().iterator().hasNext()) {
            var rels = getRelationships(data, getTimestamp(data));
            lineageStore.addRelationships(rels);
        }
    }

    public void shutdown() throws IOException {
        lineageStore.shutdown();
    }
}
