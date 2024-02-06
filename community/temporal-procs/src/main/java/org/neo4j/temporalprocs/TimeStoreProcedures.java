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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.server.rest.repr.InvalidArgumentsException;
import org.neo4j.temporalgraph.algorithms.AvgAggregator;
import org.neo4j.temporalgraph.algorithms.DynamicBFS;
import org.neo4j.temporalgraph.algorithms.DynamicCommunityDetection;
import org.neo4j.temporalgraph.algorithms.DynamicGlobalAggregation;
import org.neo4j.temporalgraph.algorithms.DynamicNodeSimilarity;
import org.neo4j.temporalgraph.algorithms.DynamicPageRankV2;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.ObjectArray;
import org.neo4j.temporalgraph.entities.RelationshipDirection;
import org.neo4j.temporalgraph.timeindex.timestore.TimeBasedTracker;
import org.neo4j.temporalprocs.result.BFSResult;
import org.neo4j.temporalprocs.result.GlobalAggregationResult;
import org.neo4j.temporalprocs.result.GraphProjectResult;
import org.neo4j.temporalprocs.result.LabelRankResult;
import org.neo4j.temporalprocs.result.MutateResult;
import org.neo4j.temporalprocs.result.NodeSimilarityResult;
import org.neo4j.temporalprocs.result.PageRankResult;
import org.neo4j.temporalprocs.result.ResultExpandedNode;
import org.neo4j.temporalprocs.result.ResultNode;
import org.neo4j.temporalprocs.result.ResultNodeWithEndTime;
import org.neo4j.temporalprocs.result.ResultRelationship;
import org.neo4j.temporalprocs.result.ResultRelationshipWithEndTime;
import org.neo4j.temporalprocs.result.ResultStore;

public class TimeStoreProcedures extends ProcBase {

    private static final String TRACKER_ERROR_MESSAGE = "No TimeBasedTracker was initialized";

    @Procedure(name = "time.getTemporalNode", mode = Mode.READ)
    @Description("Get a node at a specific time based on its id and a timestamp.")
    public Stream<ResultNode> getTemporalNode(@Name("nodeId") long nodeId, @Name("timestamp") long timestamp)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var node = tbt.getTimeStore().getNode(nodeId, timestamp);
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

    @Procedure(name = "time.expandNode", mode = Mode.READ)
    @Description("Expand a node at a specific time.")
    public Stream<ResultExpandedNode> expandNode(
            @Name("nodeId") long nodeId,
            @Name("type") long type,
            @Name("hops") long hops,
            @Name("timestamp") long timestamp)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var direction = RelationshipDirection.getDirection((int) type);
                if (direction != RelationshipDirection.OUTGOING) {
                    throw new RuntimeException(
                            String.format("Unsupported relationship direction %s", direction.toString()));
                }
                var nodes = tbt.getTimeStore().expand(nodeId, direction, (int) hops, timestamp);
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

    @Procedure(name = "time.getTemporalAllNodes", mode = Mode.READ)
    @Description("Get all node at a specific time based on a timestamp.")
    public Stream<ResultNode> getTemporalAllNodes(@Name("timestamp") long timestamp) throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var nodes = tbt.getTimeStore().getGraph(timestamp).getNodeMap();

                return getResultNodes(nodes).stream();
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.getTemporalAllNodesWithAT", mode = Mode.READ)
    @Description("Get all node at a specific time based on a timestamp.")
    public Stream<ResultNode> getTemporalAllNodesAT(
            @Name("timestamp") long timestamp,
            @Name("applicationStartTime") long atStart,
            @Name("applicationEndTime") long atEnd)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var nodes = tbt.getTimeStore()
                        .getGraph(timestamp)
                        .filterByApplicationTime(atStart, atEnd)
                        .getNodeMap();

                return getResultNodes(nodes).stream();
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.getTemporalAllRelationships", mode = Mode.READ)
    @Description("Get all relationships at a specific time based on a timestamp.")
    public Stream<ResultRelationship> getTemporalAllRelationships(@Name("timestamp") long timestamp)
            throws IOException {

        // hack: get a handle to the LineageStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var relationships = tbt.getTimeStore().getGraph(timestamp).getRelationshipMap();

                return getResultRelationships(relationships).stream();
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.temporalProjection", mode = Mode.READ)
    @Description("Create a projection at a specific time.")
    public Stream<GraphProjectResult> temporalProjection(@Name("timestamp") long timestamp)
            throws InvalidArgumentsException, IOException {
        throw new IllegalArgumentException("Unimplemented functionality");
    }

    @Procedure(name = "time.pageRank", mode = Mode.READ)
    @Description("Compute pagerank at a specific time.")
    public Stream<PageRankResult> temporalPageRank(@Name("timestamp") long timestamp) throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var pagerank = new DynamicPageRankV2();
                var graph = tbt.getTimeStore().getGraph(timestamp);
                pagerank.initialize(graph);

                var result = pagerank.getResult();
                return result.stream().map(r -> new PageRankResult(timestamp, r.left, r.right));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.dynamicPageRank", mode = Mode.READ)
    @Description("Compute pagerank incrementally.")
    public Stream<PageRankResult> temporalDynamicPageRank(
            @Name("systemStartTime") long startTime, @Name("systemEndTime") long endTime, @Name("step") long step)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                List<Pair<Long, List<MutablePair<Long, Double>>>> results = new ArrayList<>();
                var pagerank = new DynamicPageRankV2();
                var graph = tbt.getTimeStore().getGraph(startTime);
                pagerank.initialize(graph);
                results.add(ImmutablePair.of(startTime, pagerank.getResult()));

                for (; startTime < endTime; startTime += step) {
                    var upperBound = Math.min(startTime + step, endTime);
                    var diff = tbt.getTimeStore().getDiff(startTime + 1, upperBound);
                    pagerank.update(diff);
                    results.add(ImmutablePair.of(upperBound, pagerank.getResult()));
                }

                // Construct results with timestamps
                List<PageRankResult> output = new ArrayList<>();
                for (var res : results) {
                    for (var pr : res.getRight()) {
                        output.add(new PageRankResult(res.getKey(), pr.left, pr.right));
                    }
                }
                return output.stream();
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.dynamicPageRankMutate", mode = Mode.READ)
    @Description("Compute pagerank incrementally.")
    public Stream<MutateResult> temporalDynamicPageRankMutate(
            @Name("systemStartTime") long startTime, @Name("systemEndTime") long endTime, @Name("step") long step)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var pagerank = new DynamicPageRankV2();
                var graph = tbt.getTimeStore().getGraph(startTime);
                pagerank.initialize(graph);
                ResultStore.getInstance().put(DEFAULT_DATABASE_NAME + "-PR-" + startTime, pagerank.getResult());

                for (; startTime < endTime; startTime += step) {
                    var upperBound = Math.min(startTime + step, endTime);
                    var diff = tbt.getTimeStore().getDiff(startTime + 1, upperBound);
                    pagerank.update(diff);
                    ResultStore.getInstance().put(DEFAULT_DATABASE_NAME + "-PR-" + upperBound, pagerank.getResult());
                }

                return Stream.of(new MutateResult(0));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.avg", mode = Mode.READ)
    @Description("Compute avg at a specific time.")
    public Stream<GlobalAggregationResult> temporalAvg(
            @Name("property") String property, @Name("timestamp") long timestamp) throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var avg = new DynamicGlobalAggregation(new AvgAggregator(), property);
                var graph = tbt.getTimeStore().getGraph(timestamp);
                avg.initialize(graph);

                var result = avg.getResult();
                return Stream.of(new GlobalAggregationResult(timestamp, result));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.dynamicAvg", mode = Mode.READ)
    @Description("Compute avg incrementally.")
    public Stream<GlobalAggregationResult> temporalDynamicAvg(
            @Name("property") String property,
            @Name("systemStartTime") long startTime,
            @Name("systemEndTime") long endTime,
            @Name("step") long step)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                List<Pair<Long, Double>> results = new ArrayList<>();
                var avg = new DynamicGlobalAggregation(new AvgAggregator(), property);
                var graph = tbt.getTimeStore().getGraph(startTime);
                avg.initialize(graph);
                results.add(ImmutablePair.of(startTime, avg.getResult()));

                for (; startTime < endTime; startTime += step) {
                    var upperBound = Math.min(startTime + step, endTime);
                    var diff = tbt.getTimeStore().getDiff(startTime + 1, upperBound);
                    avg.update(diff);
                    results.add(ImmutablePair.of(upperBound, avg.getResult()));
                }

                return results.stream().map(r -> new GlobalAggregationResult(r.getLeft(), r.getRight()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.labelRank", mode = Mode.READ)
    @Description("Compute community detection at a specific time.")
    public Stream<LabelRankResult> temporalLabelRank(@Name("timestamp") long timestamp) throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var labelrank = new DynamicCommunityDetection();
                var graph = tbt.getTimeStore().getGraph(timestamp);
                labelrank.initialize(graph);

                var result = labelrank.getResult();
                return result.entrySet().stream().map(r -> new LabelRankResult(timestamp, r.getKey(), r.getValue()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.dynamicLabelRank", mode = Mode.READ)
    @Description("Compute community detection incrementally.")
    public Stream<LabelRankResult> temporalDynamicLabelRank(
            @Name("systemStartTime") long startTime, @Name("systemEndTime") long endTime, @Name("step") long step)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                List<Pair<Long, Map<Long, Long>>> results = new ArrayList<>();
                var labelrank = new DynamicCommunityDetection();
                var graph = tbt.getTimeStore().getGraph(startTime);
                labelrank.initialize(graph);
                results.add(ImmutablePair.of(startTime, labelrank.getResult()));

                for (; startTime < endTime; startTime += step) {
                    var upperBound = Math.min(startTime + step, endTime);
                    var diff = tbt.getTimeStore().getDiff(startTime + 1, upperBound);
                    labelrank.update(diff);
                    results.add(ImmutablePair.of(upperBound, labelrank.getResult()));
                }

                // Construct results with timestamps
                List<LabelRankResult> output = new ArrayList<>();
                for (var res : results) {
                    for (var lr : res.getRight().entrySet()) {
                        output.add(new LabelRankResult(res.getKey(), lr.getKey(), lr.getValue()));
                    }
                }
                return output.stream();
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.bfs", mode = Mode.READ)
    @Description("Compute BFS at a specific time.")
    public Stream<BFSResult> temporalBFS(@Name("nodeId") long nodeId, @Name("timestamp") long timestamp)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var bfs = new DynamicBFS(nodeId);
                var graph = tbt.getTimeStore().getGraph(timestamp);
                bfs.initialize(graph);

                var result = bfs.getResult();
                return result.stream().map(r -> new BFSResult(timestamp, r.getKey(), r.getValue()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.dynamicBfs", mode = Mode.READ)
    @Description("Compute BFS incrementally.")
    public Stream<BFSResult> temporalDynamicBFS(
            @Name("nodeId") long nodeId,
            @Name("systemStartTime") long startTime,
            @Name("systemEndTime") long endTime,
            @Name("step") long step)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                List<Pair<Long, List<Pair<Integer, Integer>>>> results = new ArrayList<>();
                var bfs = new DynamicBFS(nodeId);
                var graph = tbt.getTimeStore().getGraph(startTime);
                bfs.initialize(graph);
                results.add(ImmutablePair.of(startTime, bfs.getResult()));

                for (; startTime < endTime; startTime += step) {
                    var upperBound = Math.min(startTime + step, endTime);
                    var diff = tbt.getTimeStore().getDiff(startTime + 1, upperBound);
                    bfs.update(diff);
                    results.add(ImmutablePair.of(upperBound, bfs.getResult()));
                }

                // Construct results with timestamps
                List<BFSResult> output = new ArrayList<>();
                for (var res : results) {
                    for (var lr : res.getRight()) {
                        output.add(new BFSResult(res.getKey(), lr.getKey(), lr.getValue()));
                    }
                }
                return output.stream();
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.dynamicBfsMutate", mode = Mode.READ)
    @Description("Compute BFS incrementally.")
    public Stream<MutateResult> temporalDynamicBFSMutate(
            @Name("nodeId") long nodeId,
            @Name("systemStartTime") long startTime,
            @Name("systemEndTime") long endTime,
            @Name("step") long step)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var bfs = new DynamicBFS(nodeId);
                var graph = tbt.getTimeStore().getGraph(startTime);
                bfs.initialize(graph);
                ResultStore.getInstance().put(DEFAULT_DATABASE_NAME + "-BFS-" + startTime, bfs.getResult());

                for (; startTime < endTime; startTime += step) {
                    var upperBound = Math.min(startTime + step, endTime);
                    var diff = tbt.getTimeStore().getDiff(startTime + 1, upperBound);
                    bfs.update(diff);
                    ResultStore.getInstance().put(DEFAULT_DATABASE_NAME + "-BFS-" + upperBound, bfs.getResult());
                }
                return Stream.of(new MutateResult(0));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.nodeSimilarity", mode = Mode.READ)
    @Description("Compute node similarity at a specific time.")
    public Stream<NodeSimilarityResult> temporalNodeSimilarity(@Name("timestamp") long timestamp) throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var nodeSimilarity = new DynamicNodeSimilarity(true);
                var graph = tbt.getTimeStore().getGraph(timestamp);
                nodeSimilarity.initialize(graph);

                var result = nodeSimilarity.getResult();
                return result.stream().map(r -> new NodeSimilarityResult(timestamp, r.left, r.middle, r.right));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.dynamicNodeSimilarity", mode = Mode.READ)
    @Description("Compute pagerank incrementally.")
    public Stream<NodeSimilarityResult> temporalDynamicNodeSimilarity(
            @Name("systemStartTime") long startTime, @Name("systemEndTime") long endTime, @Name("step") long step)
            throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                List<Pair<Long, List<ImmutableTriple<Integer, Integer, Double>>>> results = new ArrayList<>();
                var nodeSimilarity = new DynamicNodeSimilarity(true);
                var graph = tbt.getTimeStore().getGraph(startTime);
                nodeSimilarity.initialize(graph);
                results.add(ImmutablePair.of(startTime, nodeSimilarity.getResult()));

                for (; startTime < endTime; startTime += step) {
                    var upperBound = Math.min(startTime + step, endTime);
                    var diff = tbt.getTimeStore().getDiff(startTime + 1, upperBound);
                    nodeSimilarity.update(diff);
                    results.add(ImmutablePair.of(upperBound, nodeSimilarity.getResult()));
                }

                // Construct results with timestamps
                List<NodeSimilarityResult> output = new ArrayList<>();
                for (var res : results) {
                    for (var ns : res.getRight()) {
                        output.add(new NodeSimilarityResult(res.getKey(), ns.left, ns.middle, ns.right));
                    }
                }
                return output.stream();
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    @Procedure(name = "time.getTemporalGraphNodes", mode = Mode.READ)
    @Description("Get all nodes from a temporal graph")
    public Stream<ResultNodeWithEndTime> getTemporalGraphNodes(
            @Name("systemStartTime") long startTime, @Name("systemEndTime") long endTime) throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var nodes =
                        tbt.getTimeStore().getTemporalGraph(startTime, endTime).getNodes();
                return nodes.stream()
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

    @Procedure(name = "time.getTemporalGraphRelationships", mode = Mode.READ)
    @Description("Get all relationships from a temporal graph")
    public Stream<ResultRelationshipWithEndTime> getTemporalGraphRelationships(
            @Name("systemStartTime") long startTime, @Name("systemEndTime") long endTime) throws IOException {

        // hack: get a handle to the TimeStore
        var eventListeners = (GlobalTransactionEventListeners) gds.getTransactionEventListeners();
        for (TransactionEventListener<?> tracker :
                eventListeners.getDatabaseTransactionEventListeners(DEFAULT_DATABASE_NAME)) {
            if (tracker instanceof TimeBasedTracker tbt) {
                var rels =
                        tbt.getTimeStore().getTemporalGraph(startTime, endTime).getRelationships();
                return rels.stream()
                        .map(inMemoryRelationship -> new ResultRelationshipWithEndTime(
                                inMemoryRelationship.getStartTimestamp(),
                                inMemoryRelationship.getEndTimestamp(),
                                inMemoryRelationship.getEntityId(),
                                inMemoryRelationship.getStartNode(),
                                inMemoryRelationship.getEndNode(),
                                inMemoryRelationship.getType(),
                                inMemoryRelationship.getProperties()));
            }
        }
        throw new IllegalStateException(TRACKER_ERROR_MESSAGE);
    }

    private List<ResultNode> getResultNodes(ObjectArray<InMemoryNode> nodes) {
        List<ResultNode> result = new ArrayList<>();
        var iterator = nodes.iterator();
        while (iterator.hasNext()) {
            var inMemoryNode = iterator.next();
            result.add(new ResultNode(
                    inMemoryNode.getStartTimestamp(),
                    inMemoryNode.getEntityId(),
                    inMemoryNode.getLabels(),
                    inMemoryNode.getProperties()));
        }
        return result;
    }

    private List<ResultRelationship> getResultRelationships(ObjectArray<InMemoryRelationship> relationships) {
        List<ResultRelationship> result = new ArrayList<>();
        var iterator = relationships.iterator();
        while (iterator.hasNext()) {
            var inMemoryRel = iterator.next();
            result.add(new ResultRelationship(
                    inMemoryRel.getStartTimestamp(),
                    inMemoryRel.getEntityId(),
                    inMemoryRel.getStartNode(),
                    inMemoryRel.getEndNode(),
                    inMemoryRel.getType(),
                    inMemoryRel.getProperties()));
        }
        return result;
    }
}
