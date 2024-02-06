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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.lineageindex.entitystores.NodeStore;

public class PersistentNodeStore implements NodeStore {
    // Key: Pair<nodeId, timestamp>, Value: RawBytes
    private final EntityStore<InMemoryNode> nodeIndex;

    public PersistentNodeStore(
            PageCache pageCache,
            FileSystemAbstraction fs,
            Path nodeIndexPath,
            Map<String, Integer> namesToIds,
            Map<Integer, String> idsToNames) {

        nodeIndex = new EntityStore<>(pageCache, fs, nodeIndexPath, namesToIds, idsToNames);
    }

    @Override
    public void addNodes(List<InMemoryNode> nodes) throws IOException {
        nodeIndex.put(nodes);
    }

    @Override
    public Optional<InMemoryNode> getNode(long nodeId, long timestamp) throws IOException {
        return nodeIndex.get(nodeId, timestamp);
    }

    @Override
    public List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) throws IOException {
        return nodeIndex.get(nodeId, startTime, endTime);
    }

    @Override
    public List<InMemoryNode> getAllNodes(long timestamp) throws IOException {
        return nodeIndex.getAll(timestamp);
    }

    @Override
    public void flushIndex() {
        nodeIndex.flushIndex();
    }

    @Override
    public void reset() {
        // todo: clear nodeIndex
    }

    @Override
    public void shutdown() throws IOException {
        nodeIndex.shutdown();
    }

    @Override
    public void setDiffThreshold(int threshold) {
        nodeIndex.setDiffThreshold(threshold);
    }
}
