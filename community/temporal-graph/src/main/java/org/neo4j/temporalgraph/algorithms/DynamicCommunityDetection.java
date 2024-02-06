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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.impl.factory.Maps;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;

public class DynamicCommunityDetection implements DynamicAlgorithm<Map<Long, Long>> {

    // Default relationship weight
    private static final double DEFAULT_WEIGHT = 1;
    // Similarity threshold used in the node selection step, values within [0, 1] => set between 0.5-0.7
    final double similarityThreshold;
    // Exponent used in the inflation step
    final double exponent;
    // Lowest probability at the cutoff step
    final double minValue;
    // Weight property name
    final String weightProperty;
    // Default self-loop weight
    final double selfLoopWeight;
    // Maximum number of iterations
    final long maxIterations;
    // Maximum number of updates for any node
    final long maxUpdates;

    // Map containing each node’s community label probabilities
    Map<Long, Map<Long, Double>> labelPs; // todo: replace with primitive maps
    // Sum of weights of each node’s in-relationships
    Map<Long, Double> sumWeights;
    /// how many times each node has been updated
    Map<Long, Long> timesUpdated;

    private static final Set<Long> EMPTY_SET = new HashSet<>();

    private InMemoryGraph graph; // todo: the graph should be updated externally
    private Map<Long, Long> labels;

    public DynamicCommunityDetection() {
        this(0.7, 4, 0.1, "weight", 1.0, 100, 5);
    }

    public DynamicCommunityDetection(
            double similarityThreshold,
            double exponent,
            double minValue,
            String weightProperty,
            double selfLoopWeight,
            long maxIterations,
            long maxUpdates) {
        this.similarityThreshold = similarityThreshold;
        this.exponent = exponent;
        this.minValue = minValue;
        this.weightProperty = weightProperty;
        this.selfLoopWeight = selfLoopWeight;
        this.maxIterations = maxIterations;
        this.maxUpdates = maxUpdates;
    }

    @Override
    public void initialize(InMemoryGraph graph) {
        this.graph = graph;

        // Initialize datastructures holding community detection state
        labelPs = Maps.mutable.empty();
        sumWeights = Maps.mutable.empty();
        timesUpdated = Maps.mutable.empty();
        labels = new HashMap<>();
        var nodeIterator = graph.getNodeMap().iterator();
        while (nodeIterator.hasNext()) {
            var node = nodeIterator.next();
            initializeNodeState(node.getEntityId());
        }

        for (long i = 0; i < maxIterations; i++) {
            var it = labelRankTIteration(false, EMPTY_SET, EMPTY_SET);
            boolean noUpdate = it.getLeft();
            long mostUpdates = it.getRight();
            if (noUpdate || mostUpdates > maxUpdates) {
                break;
            }
        }

        resetTimesUpdated();
        updateLabels();
    }

    @Override
    public void update(List<InMemoryEntity> graphUpdates) {
        Set<Long> affectedNodes = new HashSet<>();
        Set<Long> deletedNodes = new HashSet<>();
        for (var update : graphUpdates) {
            consumeUpdate(update, affectedNodes, deletedNodes);
        }

        // An additional step to ensure that deleted nodes are not added back
        for (var d : deletedNodes) {
            affectedNodes.remove(d);
        }

        recalculateLabels(affectedNodes, deletedNodes);
    }

    @Override
    public Map<Long, Long> getResult() {
        if (labels == null) {
            throw new IllegalStateException("Community detection is not computed yet");
        }
        return new HashMap<>(labels);
    }

    @Override
    public void reset() {
        graph = null;
        labelPs.clear();
        sumWeights.clear();
        timesUpdated.clear();
        labels.clear();
    }

    private void consumeUpdate(InMemoryEntity update, Set<Long> affectedNodes, Set<Long> deletedNodes) {
        if (update instanceof InMemoryNode node) {
            graph.updateNode(node);
            if (!node.isDeleted()) {
                affectedNodes.add(node.getEntityId());
            } else {
                deletedNodes.add(node.getEntityId());
            }
        } else if (update instanceof InMemoryRelationship rel) {
            graph.updateRelationship(rel);
            affectedNodes.add(rel.getStartNode());
            affectedNodes.add(rel.getEndNode());
        } else {
            throw new IllegalArgumentException(String.format("Invalid update type %s", update));
        }
    }

    private void recalculateLabels(Set<Long> affectedNodes, Set<Long> deletedNodes) {
        removeDeletedNodes(deletedNodes);
        for (var nodeId : affectedNodes) {
            initializeNodeState(nodeId);
        }

        for (long i = 0; i < maxIterations; i++) {
            var it = labelRankTIteration(true, affectedNodes, deletedNodes);
            boolean noUpdate = it.getLeft();
            long mostUpdates = it.getRight();
            if (noUpdate || mostUpdates > maxUpdates) {
                break;
            }
        }

        resetTimesUpdated();
        updateLabels();
    }

    private void removeDeletedNodes(Set<Long> deletedNodes) {
        for (var nodeId : deletedNodes) {
            labelPs.remove(nodeId);
            sumWeights.remove(nodeId);
            timesUpdated.remove(nodeId);
        }
    }

    private void updateLabels() {
        labels.clear();

        // First track the labels of each node
        Set<Long> labelsOrdered = new TreeSet<>();
        for (var entry : labelPs.entrySet()) {
            labelsOrdered.add(getLabelWithMaxProb(entry.getValue()));
        }
        labelsOrdered.remove(-1L);

        // Then assign to each label a new id beginning with 1
        long labelId = 1;
        Map<Long, Long> lookup = new HashMap<>();
        for (var label : labelsOrdered) {
            lookup.put(label, labelId);
            labelId++;
        }
        lookup.put(-1L, -1L);

        // Assemble final results
        for (var entry : labelPs.entrySet()) {
            var node = entry.getKey();
            var label = getLabelWithMaxProb(entry.getValue());
            labels.put(node, lookup.get(label));
        }
    }

    // Get the label with the highest probability
    private long getLabelWithMaxProb(Map<Long, Double> labels) {
        long label = -1;
        double maxP = 0;

        for (var entry : labels.entrySet()) {
            var l = entry.getKey();
            var p = entry.getValue();
            if (p > maxP || (p == maxP && l < label)) {
                maxP = p;
                label = l;
            }
        }

        return label;
    }

    private void initializeNodeState(long nodeId) {
        timesUpdated.put(nodeId, 0L);

        double weightSum = selfLoopWeight;
        var inRelationships = graph.getRelationships((int) nodeId, RelationshipDirection.INCOMING);
        for (var rel : inRelationships) {
            weightSum += getRelationshipWeight(rel);
        }

        Map<Long, Double> nodeLabelPs = Maps.mutable.empty();
        // add self-loop
        nodeLabelPs.put(nodeId, selfLoopWeight / weightSum);

        // add other relationships
        for (var rel : inRelationships) {
            var source = rel.getStartNode();
            nodeLabelPs.put(source, nodeLabelPs.getOrDefault(source, 0.) + (getRelationshipWeight(rel) / weightSum));
        }

        labelPs.put(nodeId, nodeLabelPs);
        sumWeights.put(nodeId, weightSum);
    }

    private void resetTimesUpdated() {
        for (var entry : timesUpdated.entrySet()) {
            entry.setValue(0L);
        }
    }

    double getRelationshipWeight(InMemoryRelationship rel) {
        for (var p : rel.getProperties()) {
            if (p.name().equals(weightProperty)) {
                return getValue(p.value());
            }
        }
        return DEFAULT_WEIGHT;
    }

    private Pair<Boolean, Long> labelRankTIteration(
            boolean isIncremental, Set<Long> affectedNodes, Set<Long> deletedNodes) {
        boolean noneUpdated = true;
        long mostUpdates = 0;

        Map<Long, Map<Long, Double>> updatedLabelPs = new HashMap<>();
        for (var entry : sumWeights.entrySet()) {
            var nodeId = entry.getKey();
            if (isIncremental) {
                var wasUpdated = affectedNodes.contains(nodeId);
                var wasDeleted = deletedNodes.contains(nodeId);
                if (!wasUpdated || wasDeleted) {
                    continue;
                }
            }

            // conditional update
            if (!distinctEnough(nodeId)) {
                continue;
            }
            noneUpdated = false;

            // label propagation
            Map<Long, Double> updatedNodeLabelPs = propagate(nodeId);

            // inflation
            inflate(updatedNodeLabelPs);

            // cutoff
            cutoff(updatedNodeLabelPs);

            updatedLabelPs.put(nodeId, updatedNodeLabelPs);
        }

        for (var entry : updatedLabelPs.entrySet()) {
            var nodeId = entry.getKey();
            var updatedNodeLabelPs = entry.getValue();
            labelPs.put(nodeId, updatedNodeLabelPs);
            var currentUpdates = timesUpdated.getOrDefault(nodeId, 0L) + 1;
            timesUpdated.put(nodeId, currentUpdates);

            if (currentUpdates > mostUpdates) {
                mostUpdates = currentUpdates;
            }
        }

        return ImmutablePair.of(noneUpdated, mostUpdates);
    }

    // Checks if given node’s label probabilities are sufficiently distinct
    // from its neighbors’.
    // For a label probability vector to be considered sufficiently distinct, it
    // has to be a subset of less than k% label probability vectors of its
    // neighbors.
    private boolean distinctEnough(long nodeId) {
        var nodeLabels = mostProbableLabels(nodeId);
        long labelSimilarity = 0;

        var inRelationships = graph.getRelationships((int) nodeId, RelationshipDirection.INCOMING);
        for (var rel : inRelationships) {
            var source = rel.getStartNode();
            var neighbourLabels = mostProbableLabels(source);
            // nodeLabels is subset of neighbourLabels
            if (neighbourLabels.containsAll(nodeLabels)) {
                labelSimilarity++;
            }
        }

        int inDegree = inRelationships.size();

        return labelSimilarity <= inDegree * similarityThreshold;
    }

    private Set<Long> mostProbableLabels(long nodeId) {
        double maxP = 0;
        var localLPs = labelPs.get(nodeId);
        for (var p : localLPs.values()) {
            if (p > maxP) {
                maxP = p;
            }
        }

        Set<Long> mostProbableLabels = new HashSet<>();
        for (var entry : localLPs.entrySet()) {
            var label = entry.getKey();
            var p = entry.getValue();
            if (p == maxP) {
                mostProbableLabels.add(label);
            }
        }

        return mostProbableLabels;
    }

    // Performs label propagation on given node’s label probability vector.
    // Label propagation works by calculating a weighted sum of the label
    // probability vectors of the node’s neighbors.
    private Map<Long, Double> propagate(long nodeId) {
        Map<Long, Double> newLabelPs = Maps.mutable.empty();

        // propagate own probabilities (handle self-loops)
        for (var entry : labelPs.get(nodeId).entrySet()) {
            var label = entry.getKey();
            var p = entry.getValue();
            newLabelPs.put(label, selfLoopWeight / sumWeights.get(nodeId) * p);
        }

        // propagate neighbors’ probabilities
        var inRelationships = graph.getRelationships((int) nodeId, RelationshipDirection.INCOMING);
        for (var rel : inRelationships) {
            var source = rel.getStartNode();
            final double contribution = getRelationshipWeight(rel) / sumWeights.get(nodeId);
            for (var entry : labelPs.get(source).entrySet()) {
                var label = entry.getKey();
                var p = entry.getValue();
                newLabelPs.put(label, newLabelPs.getOrDefault(label, 0.) + contribution * p);
            }
        }

        return newLabelPs;
    }

    // Raises given node’s label probabilities to specified power and
    // normalizes the result.
    private void inflate(Map<Long, Double> nodeLabelPs) {
        double sumPs = 0;
        for (var entry : nodeLabelPs.entrySet()) {
            var label = entry.getKey();
            final double inflatedNodeLabelPs = Math.pow(nodeLabelPs.get(label), exponent);
            entry.setValue(inflatedNodeLabelPs);
            sumPs += inflatedNodeLabelPs;
        }

        for (var entry : nodeLabelPs.entrySet()) {
            var label = entry.getKey();
            nodeLabelPs.put(label, nodeLabelPs.get(label) / sumPs);
        }
    }

    // Removes values under a set threshold from given node’s label
    //  probability vector.
    private void cutoff(Map<Long, Double> nodeLabelPs) {
        var iter = nodeLabelPs.entrySet().iterator();
        while (iter.hasNext()) {
            var entry = iter.next();
            var p = entry.getValue();
            if (p < minValue) {
                iter.remove();
            }
        }
    }

    private double getValue(Object obj) {
        if (obj instanceof Integer i) {
            return Double.valueOf(i);
        } else if (obj instanceof Long l) {
            return Double.valueOf(l);
        } else if (obj instanceof Double d) {
            return d;
        } else if (obj instanceof Float f) {
            return Double.valueOf(f);
        } else {
            return DEFAULT_WEIGHT;
        }
    }
}
