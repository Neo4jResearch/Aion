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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;

public class DynamicPageRankV2 implements DynamicAlgorithm<List<MutablePair<Long, Double>>> {

    /* PageRank specific data */
    private static final int INITIAL_SIZE = 1024;
    private double[] currentRanks;
    private double[] nextRanks;
    private byte[] activeNodes;
    private byte[] affectedNodes;
    private boolean hasWork;
    private final double epsilon;
    private static final int MAXIMUM_ITERATIONS = 100;

    private List<MutablePair<Long, Double>> pageRanks;

    private InMemoryGraph graph; // todo: the graph should be updated externally

    public DynamicPageRankV2() {
        this(0.01d);
    }

    public DynamicPageRankV2(double epsilon) {
        this.currentRanks = new double[INITIAL_SIZE];
        this.nextRanks = new double[INITIAL_SIZE];
        this.activeNodes = new byte[INITIAL_SIZE];
        this.affectedNodes = new byte[INITIAL_SIZE];
        this.hasWork = true;
        this.epsilon = epsilon;
        this.pageRanks = null;
        this.graph = null;
    }

    @Override
    public void initialize(InMemoryGraph graph) {
        this.graph = graph;
        pageRanks = initializeAndRunPageRank(false);
    }

    @Override
    public void update(List<InMemoryEntity> graphUpdates) {
        Arrays.fill(affectedNodes, (byte) 0);

        // First consume relationships
        for (var update : graphUpdates) {
            if (update instanceof InMemoryRelationship rel) {
                graph.updateRelationship(rel);
                updateRelationship(rel);
            }
        }
        // Then, node updates
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
        swapBitmaps();

        pageRanks = initializeAndRunPageRank(true);
    }

    @Override
    public List<MutablePair<Long, Double>> getResult() {
        if (pageRanks == null) {
            throw new IllegalStateException("PageRank is not computed yet");
        }
        return pageRanks;
    }

    @Override
    public void reset() {
        graph = null;
        Arrays.fill(activeNodes, (byte) 0);
        Arrays.fill(affectedNodes, (byte) 0);
        pageRanks.clear();
    }

    private void normalizeRank(List<MutablePair<Long, Double>> rank) {
        double sum = rank.stream().mapToDouble(Pair::getRight).sum();
        for (var r : rank) {
            r.setRight(r.getRight() / sum);
        }
    }

    private List<MutablePair<Long, Double>> calculatePageRank() {
        var iteration = 0;
        while (hasWork && iteration < MAXIMUM_ITERATIONS) {

            hasWork = false;
            Arrays.fill(nextRanks, 0.);
            for (var nodeId = 0; nodeId < activeNodes.length; ++nodeId) {
                if (activeNodes[nodeId] == 0 || !graph.containsNode(nodeId)) {
                    continue;
                }

                var iterator = graph.getIncomingRelationships(nodeId).intIterator();
                while (iterator.hasNext()) {
                    var relId = iterator.next();
                    var rel = graph.getRelationship(relId).get();
                    var sourceId = (int) rel.getStartNode();
                    var outgoingRelCount = graph.getRelationshipsCount(sourceId, RelationshipDirection.OUTGOING);
                    var contribution = currentRanks[sourceId] / outgoingRelCount;
                    nextRanks[nodeId] += contribution;
                }

                nextRanks[nodeId] = 0.15 + 0.85 * nextRanks[nodeId];
                if (Math.abs(nextRanks[nodeId] - currentRanks[nodeId]) > epsilon) {
                    // propagate changes to neighbours
                    iterator = graph.getOutgoingRelationships(nodeId).intIterator();
                    while (iterator.hasNext()) {
                        var relId = iterator.next();
                        var rel = graph.getRelationship(relId).get();
                        var targetId = (int) rel.getEndNode();
                        affectedNodes[targetId] = 1;
                        hasWork = true;
                    }
                }
            }

            for (var nodeId = 0; nodeId < activeNodes.length; ++nodeId) {
                if (activeNodes[nodeId] == 0 || !graph.containsNode(nodeId)) {
                    continue;
                }
                currentRanks[nodeId] = nextRanks[nodeId];
            }

            Arrays.fill(activeNodes, (byte) 0);
            swapBitmaps();

            iteration++;
        }

        List<MutablePair<Long, Double>> pRanks = new ArrayList<>();
        var nodeIterator = graph.getNodeMap().iterator();
        while (nodeIterator.hasNext()) {
            var node = nodeIterator.next();
            var nodeId = (int) node.getEntityId();
            pRanks.add(new MutablePair<>((long) nodeId, currentRanks[nodeId]));
        }

        normalizeRank(pRanks);
        return pRanks;
    }

    private List<MutablePair<Long, Double>> initializeAndRunPageRank(boolean hasPreviousValues) {
        var maxNodeId = graph.getNodeMap().maxSize();
        if (maxNodeId >= currentRanks.length) {
            var newSize = maxNodeId + 1;

            var newCR = new double[newSize];
            var newAN = new byte[newSize];
            if (hasPreviousValues) { // copy previous values
                System.arraycopy(currentRanks, 0, newCR, 0, currentRanks.length);
                System.arraycopy(activeNodes, 0, newAN, 0, activeNodes.length);
            }
            currentRanks = newCR;
            activeNodes = newAN;

            nextRanks = new double[newSize];
            affectedNodes = new byte[newSize];
        }

        // Initialize active nodes and current ranks
        if (!hasPreviousValues) {
            Arrays.fill(activeNodes, 0, maxNodeId, (byte) 1);
            Arrays.fill(currentRanks, 1.);
            hasWork = true;
        }

        return calculatePageRank();
    }

    private void addNode(int nodeId) {
        affectedNodes[nodeId] = 1;
        var iterator = graph.getOutgoingRelationships(nodeId).intIterator();
        while (iterator.hasNext()) {
            var relId = iterator.next();
            var rel = graph.getRelationship(relId).get();
            var targetId = (int) rel.getEndNode();
            affectedNodes[targetId] = 1;
            hasWork = true;
        }
    }

    private void deleteNode(int nodeId) {
        var iterator = graph.getIncomingRelationships(nodeId).intIterator();
        while (iterator.hasNext()) {
            var relId = iterator.next();
            var rel = graph.getRelationship(relId).get();
            var sourceId = (int) rel.getStartNode();
            affectedNodes[sourceId] = 1;
            hasWork = true;
        }

        iterator = graph.getOutgoingRelationships(nodeId).intIterator();
        while (iterator.hasNext()) {
            var relId = iterator.next();
            var rel = graph.getRelationship(relId).get();
            var targetId = (int) rel.getEndNode();
            affectedNodes[targetId] = 1;
            hasWork = true;
        }
    }

    private void updateRelationship(InMemoryRelationship rel) {
        var sourceId = (int) rel.getStartNode();
        // var targetId = (int) rel.getStartNode();
        affectedNodes[sourceId] = 1;
        // affectedNodes[targetId] = 1;
        hasWork = true;
    }

    private void swapBitmaps() {
        var temp = activeNodes;
        activeNodes = affectedNodes;
        affectedNodes = temp;
    }

    private void swapRanks() {
        var temp = currentRanks;
        currentRanks = nextRanks;
        nextRanks = temp;
    }
}
