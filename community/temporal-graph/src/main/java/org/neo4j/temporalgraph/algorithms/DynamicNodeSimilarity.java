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
package org.neo4j.temporalgraph.algorithms;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.tuple.primitive.IntDoublePair;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.ObjectArray;
import org.neo4j.temporalgraph.utils.ThreadPool;

public class DynamicNodeSimilarity implements DynamicAlgorithm<List<ImmutableTriple<Integer, Integer, Double>>> {
    /* NodeSimilarity specific data */
    private static final int INITIAL_SIZE = 1024;
    private static final int TOP_K = 10;
    private static final double SIMILARITY_CUTOFF = 1E-42;
    private final boolean isParallel;

    private BitSet activeNodes;
    private final ThreadLocal<BitSet> set = ThreadLocal.withInitial(BitSet::newInstance);
    private Map<Integer, IntDoublePair[]> state;
    private ObjectArray<MutableIntList> adjLists;
    boolean hasWork;
    private List<ImmutableTriple<Integer, Integer, Double>> nodeSimilarities;
    private InMemoryGraph graph; // todo: the graph should be updated externally

    public DynamicNodeSimilarity(boolean isParallel) {
        this.isParallel = isParallel;
    }

    @Override
    public void initialize(InMemoryGraph graph) {
        this.graph = graph;
        this.activeNodes = new BitSet(INITIAL_SIZE);
        this.state = new ConcurrentHashMap<>();
        this.adjLists = new ObjectArray<>(INITIAL_SIZE);
        this.nodeSimilarities = new ArrayList<>();

        resizeAndResetActiveNodes();
        prepare();
        this.hasWork = true;
        calculateNodeSimilarity();
    }

    @Override
    public void update(List<InMemoryEntity> graphUpdates) {
        // Track all affected nodes
        resizeAndResetActiveNodes();

        // var sizeBeforeUpdates = state.size();
        // First consume node updates
        for (var update : graphUpdates) {
            if (update instanceof InMemoryNode node) {
                graph.updateNode(node);
                if (!node.isDeleted()) {
                    addNode((int) node.getEntityId());
                } else {
                    deleteNode((int) node.getEntityId());
                }
            }
        }
        // Then, consume relationships
        for (var update : graphUpdates) {
            if (update instanceof InMemoryRelationship rel) {
                graph.updateRelationship(rel);
                updateRelationship(rel);
                // todo: handle deletions
            }
        }

        // var sizeAfterUpdates = state.size();
        // System.out.println(String.format("Active nodes %d", activeNodes.cardinality()));
        // System.out.println(String.format(
        //        "Remaining %d, out of %d (%f)",
        //        sizeAfterUpdates, sizeBeforeUpdates, (double) sizeAfterUpdates / (double) sizeBeforeUpdates));

        prepareAdjLists();
        calculateNodeSimilarity();
    }

    @Override
    public List<ImmutableTriple<Integer, Integer, Double>> getResult() {
        if (nodeSimilarities == null) {
            throw new IllegalStateException("NodeSimilarity is not computed yet");
        }
        return nodeSimilarities;
    }

    @Override
    public void reset() {
        activeNodes.clear();
        state.clear();
        hasWork = false;
        nodeSimilarities.clear();
        graph = null;
    }

    // Compute node similarity assuming a bipartite graph (see inbound checks below).
    private void calculateNodeSimilarity() {
        if (hasWork) {
            // Assume that visitedNodePairs is reset when reaching this point
            if (!isParallel) {
                singleThreadedNodeSimilarity();
            } else {
                parallelNodeSimilarity();
            }

            // Reconstruct result
            constructResult();
            hasWork = false;
        }
    }

    private void prepare() {
        var nodeIter1 = graph.getNodeMap().iterator();
        while (nodeIter1.hasNext()) {
            var node1 = nodeIter1.next();
            var nodeId1 = (int) node1.getEntityId();
            if (!inboundCheck(nodeId1)) {
                activeNodes.set(nodeId1);

                // Create adj list
                createAdjList(nodeId1);
            }
        }
        // System.out.println(String.format("Active nodes %d", activeNodes.cardinality()));
    }

    private void prepareAdjLists() {
        var iter = activeNodes.iterator();
        var nodeId = iter.nextSetBit();
        while (nodeId != BitSetIterator.NO_MORE) {
            createAdjList(nodeId);
            nodeId = iter.nextSetBit();
        }
    }

    private void createAdjList(int nodeId) {
        var rels = IntLists.mutable.empty();
        var outgoing = graph.getOutgoingRelationships(nodeId);
        for (int i = 0; i < outgoing.size(); ++i) {
            var relId = outgoing.get(i);
            var targetId = (int) graph.getRelationship(relId).get().getEndNode();
            rels.add(targetId);
        }
        adjLists.put(nodeId, rels);
    }

    private void singleThreadedNodeSimilarity() {
        // Resize set if graph increased in size
        resizeSet();

        var nodeIter1 = activeNodes.iterator();
        var nodeId1 = nodeIter1.nextSetBit();
        while (nodeId1 != BitSetIterator.NO_MORE) {
            singleNodeSimilarity(nodeId1);
            nodeId1 = nodeIter1.nextSetBit();
        }
    }

    private void parallelNodeSimilarity() {
        var threadPool = ThreadPool.getInstance();
        List<Callable<Boolean>> callableTasks = new ArrayList<>();
        final int numberOfWorkers = ThreadPool.THREAD_NUMBER;
        for (int i = 0; i < numberOfWorkers; ++i) {
            final var finalI = i;
            callableTasks.add(() -> {

                // Resize set if graph increased in size
                resizeSet();

                var nodeIter1 = activeNodes.iterator();
                var nodeId1 = nodeIter1.nextSetBit();
                while (nodeId1 != BitSetIterator.NO_MORE) {
                    if ((nodeId1 % numberOfWorkers) == finalI) {
                        singleNodeSimilarity(nodeId1);
                    }
                    nodeId1 = nodeIter1.nextSetBit();
                }

                return true;
            });
        }

        try {
            var futures = threadPool.invokeAll(callableTasks);
            for (var f : futures) {
                f.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void resizeSet() {
        if (set.get().size() < graph.getNodeMap().maxSize()) {
            set.set(new BitSet(graph.getNodeMap().maxSize()));
        }
    }

    private void singleNodeSimilarity(int nodeId1) {
        IntDoublePair[] topKSimilarities = new IntDoublePair[TOP_K];
        int topKCounter = 0;

        var union = buildSet(nodeId1);
        var nodeIter2 = activeNodes.iterator();
        var nodeId2 = nodeIter2.nextSetBit();
        while (nodeId2 != BitSetIterator.NO_MORE) {
            if (nodeId2 != nodeId1) {
                // Compute similarity
                var similarity = jaccardSimilarity(nodeId2, union);
                if (similarity >= SIMILARITY_CUTOFF) {
                    if (topKCounter < TOP_K) {
                        topKSimilarities[topKCounter] = PrimitiveTuples.pair(nodeId2, similarity);
                        topKCounter++;
                    } else {
                        updateTopK(topKSimilarities, nodeId2, similarity);
                    }
                }
            }
            nodeId2 = nodeIter2.nextSetBit();
        }
        // Store it back to the state table
        state.put(nodeId1, topKSimilarities);

        // Clear the set for the next node
        resetSet(nodeId1);
    }

    private int buildSet(int nodeId) {
        var s = set.get();
        var outgoing = graph.getOutgoingRelationships(nodeId);
        for (int i = 0; i < outgoing.size(); ++i) {
            var relId = outgoing.get(i);
            var targetId = (int) graph.getRelationship(relId).get().getEndNode();
            s.set(targetId);
        }
        return outgoing.size();
    }

    private void resetSet(int nodeId) {
        var s = set.get();
        var outgoing = graph.getOutgoingRelationships(nodeId);
        for (int i = 0; i < outgoing.size(); ++i) {
            var relId = outgoing.get(i);
            var targetId = (int) graph.getRelationship(relId).get().getEndNode();
            s.clear(targetId);
        }
    }

    private void updateTopK(IntDoublePair[] topKSimilarities, int nodeId, double similarity) {
        int index = -1;
        double prev = Double.MAX_VALUE;
        for (int i = 0; i < topKSimilarities.length; ++i) {
            var sim = topKSimilarities[i].getTwo();
            if (sim < similarity && prev > sim) {
                index = i;
                prev = sim;
            }
        }

        if (index != -1) {
            topKSimilarities[index] = PrimitiveTuples.pair(nodeId, similarity);
        }
    }

    private void constructResult() {
        nodeSimilarities.clear();
        for (var entry : state.entrySet()) {
            var nodeId = entry.getKey();
            for (var similarity : entry.getValue()) {
                if (similarity == null) {
                    break;
                }
                nodeSimilarities.add(ImmutableTriple.of(nodeId, similarity.getOne(), similarity.getTwo()));
            }
        }
    }

    private void addNode(int nodeId) {
        state.remove(nodeId);
        updateNeighbours(nodeId);
        activeNodes.set(nodeId);
    }

    private void deleteNode(int nodeId) {
        state.remove(nodeId);
        updateNeighbours(nodeId);
        activeNodes.clear(nodeId);
    }

    private void updateNeighbours(int nodeId) {
        var iterator = graph.getIncomingRelationships(nodeId).intIterator();
        while (iterator.hasNext()) {
            var relId = iterator.next();
            var rel = graph.getRelationship(relId).get();
            var sourceId = (int) rel.getStartNode();
            activeNodes.set(sourceId);
            state.remove(sourceId);
            hasWork = true;
        }

        iterator = graph.getOutgoingRelationships(nodeId).intIterator();
        while (iterator.hasNext()) {
            var relId = iterator.next();
            var rel = graph.getRelationship(relId).get();
            var targetId = (int) rel.getEndNode();
            activeNodes.set(targetId);
            state.remove(targetId);
            hasWork = true;
        }
    }

    private void updateRelationship(InMemoryRelationship rel) {
        var targetId = (int) rel.getEndNode();
        // state.remove(targetId);
        // activeNodes.set(targetId);

        // Also mark its incoming neighbours
        var iterator = graph.getIncomingRelationships(targetId).intIterator();
        while (iterator.hasNext()) {
            var relId = iterator.next();
            var r = graph.getRelationship(relId).get();
            var sourceId = (int) r.getStartNode();
            activeNodes.set(sourceId);
            state.remove(sourceId);
            hasWork = true;
        }
    }

    private void resizeAndResetActiveNodes() {
        var maxNodeId = graph.getNodeMap().maxSize();
        if (maxNodeId >= activeNodes.length()) {
            var newSize = maxNodeId + 1;
            activeNodes = new BitSet(newSize);
        }
        activeNodes.clear();
    }

    private double jaccardSimilarity(int nodeId2, int union) {
        var s = set.get();
        var intersection = 0;

        // Given that we expect a bipartite graph, we expect both node1 and node2 to
        // have only outgoing relationships
        var rels = getOutgoingRelationships(nodeId2);
        var iter = rels.intIterator();
        while (iter.hasNext()) {
            var rel = iter.next();
            if (s.get(rel)) {
                intersection++;
            } else {
                union++;
            }
        }

        if (intersection == 0 || union == 0) {
            return 0.0;
        }
        return (double) intersection / (double) union;
    }

    private MutableIntList getOutgoingRelationships(int nodeId) {
        return adjLists.get(nodeId);
    }

    private boolean inboundCheck(int nodeId) {
        return graph.getOutgoingRelationships(nodeId).isEmpty();
    }
}
