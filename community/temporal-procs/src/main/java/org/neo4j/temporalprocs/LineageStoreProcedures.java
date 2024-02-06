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
package org.neo4j.temporalprocs;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.util.stream.Stream;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.server.rest.repr.InvalidArgumentsException;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.lineageindex.EntityLineageTracker;
import org.neo4j.temporalprocs.result.GraphProjectResult;
import org.neo4j.temporalprocs.result.ResultExpandedNode;
import org.neo4j.temporalprocs.result.ResultNode;
import org.neo4j.temporalprocs.result.ResultNodeWithEndTime;
import org.neo4j.temporalprocs.result.ResultRelationship;

public class LineageStoreProcedures extends ProcBase {

    private static final String TRACKER_ERROR_MESSAGE = "No EntityLineageTracker was initialized";

    @Procedure(name = "lineage.getTemporalNode", mode = Mode.READ)
    @Description("Get a node at a specific time based on its id and a timestamp.")
    public Stream<ResultNode> getTemporalNode(@Name("nodeId") long nodeId, @Name("timestamp") long timestamp)
            throws IOException {

        // hack: get a handle to the LineageStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof EntityLineageTracker elt) {
                var node = elt.getLineageStore().getNode(nodeId, timestamp);
                return node.stream()
                        .map(inMemoryNode -> new ResultNode(
                                inMemoryNode.getStartTimestamp(),
                                inMemoryNode.getEntityId(),
                                inMemoryNode.getLabels(),
                                inMemoryNode.getProperties()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "lineage.getTemporalNodeHistory", mode = Mode.READ)
    @Description("Get a node at a specific time based on its id and a timestamp.")
    public Stream<ResultNodeWithEndTime> getTemporalNodeHistory(
            @Name("nodeId") long nodeId, @Name("systemStartTime") long startTime, @Name("systemEndTime") long endTime)
            throws IOException {

        // hack: get a handle to the LineageStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof EntityLineageTracker elt) {
                var node = elt.getLineageStore().getNode(nodeId, startTime, endTime);
                return node.stream()
                        .map(inMemoryNode -> new ResultNodeWithEndTime(
                                inMemoryNode.getStartTimestamp(),
                                inMemoryNode.getEndTimestamp(),
                                inMemoryNode.getEntityId(),
                                inMemoryNode.getLabels(),
                                inMemoryNode.getProperties()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "lineage.getTemporalAllNodes", mode = Mode.READ)
    @Description("Get all node at a specific time based on a timestamp.")
    public Stream<ResultNode> getTemporalAllNodes(@Name("timestamp") long timestamp) throws IOException {

        // hack: get a handle to the LineageStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof EntityLineageTracker elt) {
                var nodes = elt.getLineageStore().getAllNodes(timestamp);
                return nodes.stream()
                        .map(inMemoryNode -> new ResultNode(
                                inMemoryNode.getStartTimestamp(),
                                inMemoryNode.getEntityId(),
                                inMemoryNode.getLabels(),
                                inMemoryNode.getProperties()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "lineage.getTemporalRelationship", mode = Mode.READ)
    @Description("Get a relationship at a specific time based on its id and a timestamp.")
    public Stream<ResultRelationship> getTemporalRelationship(
            @Name("relId") long relId, @Name("timestamp") long timestamp) throws IOException {

        // hack: get a handle to the LineageStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof EntityLineageTracker elt) {
                var relationship = elt.getLineageStore().getRelationship(relId, timestamp);
                return relationship.stream()
                        .map(inMemoryRel -> new ResultRelationship(
                                inMemoryRel.getStartTimestamp(),
                                inMemoryRel.getEntityId(),
                                inMemoryRel.getStartNode(),
                                inMemoryRel.getEndNode(),
                                inMemoryRel.getType(),
                                inMemoryRel.getProperties()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "lineage.getTemporalAllRelationships", mode = Mode.READ)
    @Description("Get all relationships at a specific time based on a timestamp.")
    public Stream<ResultRelationship> getTemporalAllRelationships(@Name("timestamp") long timestamp)
            throws IOException {

        // hack: get a handle to the LineageStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof EntityLineageTracker elt) {
                var relationships = elt.getLineageStore().getAllRelationships(timestamp);
                return relationships.stream()
                        .map(inMemoryRel -> new ResultRelationship(
                                inMemoryRel.getStartTimestamp(),
                                inMemoryRel.getEntityId(),
                                inMemoryRel.getStartNode(),
                                inMemoryRel.getEndNode(),
                                inMemoryRel.getType(),
                                inMemoryRel.getProperties()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "lineage.expandNode", mode = Mode.READ)
    @Description("Expand a node at a specific time.")
    public Stream<ResultExpandedNode> expandNode(
            @Name("nodeId") long nodeId,
            @Name("type") long type,
            @Name("hops") long hops,
            @Name("timestamp") long timestamp)
            throws IOException {

        // hack: get a handle to the LineageStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof EntityLineageTracker elt) {
                var direction = RelationshipDirection.getDirection((int) type);
                if (direction != RelationshipDirection.OUTGOING) {
                    throw new RuntimeException(
                            String.format("Unsupported relationship direction %s", direction.toString()));
                }
                var nodes = elt.getLineageStore().expand(nodeId, direction, (int) hops, timestamp);
                return nodes.stream()
                        .map(inMemoryNode -> new ResultExpandedNode(
                                inMemoryNode.getStartTimestamp(),
                                inMemoryNode.getHop(),
                                inMemoryNode.getEntityId(),
                                inMemoryNode.getLabels(),
                                inMemoryNode.getProperties()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "lineage.temporalProjection", mode = Mode.READ)
    @Description("Create a projection at a specific time.")
    public Stream<GraphProjectResult> temporalProjection(@Name("timestamp") long timestamp)
            throws InvalidArgumentsException, IOException {
        throw new IllegalArgumentException("Unimplemented functionality");
    }
}
