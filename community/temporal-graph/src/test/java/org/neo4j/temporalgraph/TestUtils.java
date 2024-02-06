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
package org.neo4j.temporalgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.config;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

import java.io.IOException;
import java.util.List;
import org.neo4j.graphdb.Label;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.timeindex.timestore.TimeStore;
import org.neo4j.test.utils.TestDirectory;

public class TestUtils {
    public static final String TEST_DB = "testDB";
    public static final Label label = Label.label("User");

    private TestUtils() {}

    public static void checkNodeExists(HistoryStore store, InMemoryNode node, long timestamp) throws IOException {
        var resultNode = store.getNode(node.getEntityId(), timestamp);
        assertTrue(resultNode.isPresent());
        assertEquals(node, resultNode.get());
    }

    public static void checkNodeNotPresent(HistoryStore store, long nodeId, long timestamp) throws IOException {
        var resultNode = store.getNode(nodeId, timestamp);
        assertTrue(resultNode.isEmpty());
    }

    public static void checkRelationshipExists(
            HistoryStore store, InMemoryRelationship expectedRelationship, long timestamp) throws IOException {
        var resultRel = store.getRelationship(expectedRelationship.getEntityId(), timestamp);
        assertTrue(resultRel.isPresent());
        assertEquals(expectedRelationship, resultRel.get());
    }

    public static void checkRelationshipNotPresent(HistoryStore store, long relId, long timestamp) throws IOException {
        var resultNode = store.getRelationship(relId, timestamp);
        assertTrue(resultNode.isEmpty());
    }

    public static void compareGraphsWithoutRelationshipPointers(InMemoryGraph lhs, InMemoryGraph rhs) {
        for (var n : lhs.getNodes()) {
            var rhsNode = rhs.getNode((int) n.getEntityId());
            assertTrue(rhsNode.isPresent());
            assertEquals(n, rhsNode.get());
        }
        for (var r : lhs.getRelationships()) {
            var rhsRel = rhs.getRelationship((int) r.getEntityId());
            assertTrue(rhsRel.isPresent());
            assertTrue(rhsRel.get().equalsWithoutPointers(r));
        }
    }

    public static void checkAllNodesExist(TimeStore store, List<InMemoryNode> expectedNodes, long timestamp)
            throws IOException {
        var graph = store.getGraph(timestamp);
        for (var expectedNode : expectedNodes) {
            var resultNode = graph.getNode((int) expectedNode.getEntityId());
            assertTrue(resultNode.isPresent());
            assertEquals(expectedNode, resultNode.get());
        }
    }

    public static void checkAllRelationshipsExist(
            TimeStore store, List<InMemoryRelationship> expectedRelationships, long timestamp) throws IOException {
        var graph = store.getGraph(timestamp);
        for (var expectedRelationship : expectedRelationships) {
            var resultRelationship = graph.getRelationship((int) expectedRelationship.getEntityId());
            assertTrue(resultRelationship.isPresent());
            assertEquals(expectedRelationship, resultRelationship.get());
        }
    }

    public static PageCache getPageCache(TestDirectory directory) {
        final int pageSize = (int) kibiBytes(16);
        var memoryAllocator = MemoryAllocator.createAllocator(mebiBytes(128), EmptyMemoryTracker.INSTANCE);
        MuninnPageCache.Configuration configuration =
                config(memoryAllocator).pageCacheTracer(PageCacheTracer.NULL).pageSize(pageSize);
        return StandalonePageCacheFactory.createPageCache(
                directory.getFileSystem(), createInitialisedScheduler(), PageCacheTracer.NULL, configuration);
    }
}
