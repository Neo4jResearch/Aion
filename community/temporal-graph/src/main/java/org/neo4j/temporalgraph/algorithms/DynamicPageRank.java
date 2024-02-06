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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.RelationshipDirection;

public class DynamicPageRank implements DynamicAlgorithm<List<MutablePair<Long, Double>>> {

    /* PageRank specific data */
    // Number of random walks per node
    private final long randomWalks;
    // Walk stopping probability
    private final double epsilon;
    /// Keeping the information about walks on the graph
    private final List<MutableLongList> walks;
    // Keeping the information of walk appearance in algorithm for faster calculation
    private final MutableLongLongMap walksCounter;
    /// Table that keeps the node appearance and walk ID
    private final Map<Long, MutableLongSet> walksTable;

    private List<MutablePair<Long, Double>> pageRanks;

    private InMemoryGraph graph; // todo: the graph should be updated externally

    public DynamicPageRank() {
        this(10, 0.2);
    }

    /**
     *
     * @param randomWalks number of random walks per node
     * @param epsilon walk stopping probability (i.e. average walk length is 1/ɛ)
     */
    public DynamicPageRank(long randomWalks, double epsilon) {
        this.randomWalks = randomWalks;
        this.epsilon = epsilon;
        this.walks = Lists.mutable.empty();
        this.walksCounter = new LongLongHashMap();
        this.walksTable = Maps.mutable.empty();

        this.pageRanks = null;
        this.graph = null;
    }

    @Override
    public void initialize(InMemoryGraph graph) {
        this.graph = graph;
        pageRanks = initializePageRank();
    }

    @Override
    public void update(List<InMemoryEntity> graphUpdates) {
        for (var update : graphUpdates) {
            // todo: Do we need to consume edge deletions before node deletions?
            consumeUpdate(update);
        }

        pageRanks = calculatePageRank();
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
        walks.clear();
        walksCounter.clear();
        walksTable.clear();
    }

    private static int getRandomInt(int from, int to) {
        return ThreadLocalRandom.current().nextInt(from, to);
    }

    private static float getRandomFloat() {
        return ThreadLocalRandom.current().nextFloat();
    }

    private void normalizeRank(List<MutablePair<Long, Double>> rank) {
        double sum = rank.stream().mapToDouble(Pair::getRight).sum();
        for (var r : rank) {
            r.setRight(r.getRight() / sum);
        }
    }

    private List<MutablePair<Long, Double>> calculatePageRank() {
        var n = walksCounter.size();
        List<MutablePair<Long, Double>> pRanks = new ArrayList<>(n);
        walksCounter.forEachKeyValue((nodeId, total) -> {
            var rank = total / ((n * randomWalks) / epsilon);
            pRanks.add(new MutablePair<>(nodeId, rank));
        });

        normalizeRank(pRanks);
        return pRanks;
    }

    private void createRoute(long nodeId, MutableLongList walk, long walkIndex, double eps) {
        var currentId = nodeId;
        while (true) {
            // Should the direction be user defined instead of outgoing?
            var neighbours = graph.getRelationships((int) currentId, RelationshipDirection.OUTGOING);
            if (neighbours.isEmpty()) {
                break;
            }

            // Pick and add the random outer relationship
            var neighboursSize = neighbours.size();
            var nextId = neighbours.get(getRandomInt(0, neighboursSize)).getEndNode();

            walk.add(nextId);
            updateWalksState(nextId, walkIndex);

            // Finish walk when random number is smaller than epsilon
            // Average length of walk is 1/epsilon
            if (getRandomFloat() < eps) {
                break;
            }

            currentId = nextId;
        }
    }

    private void updateWalksState(long nodeId, long walkIndex) {
        var set = walksTable.getOrDefault(nodeId, new LongHashSet());
        set.add(walkIndex);
        walksTable.put(nodeId, set);
        walksCounter.put(nodeId, walksCounter.getIfAbsent(nodeId, 0L) + 1L);
    }

    private List<MutablePair<Long, Double>> initializePageRank() {
        var walkIndex = 0L;
        var nodeIterator = graph.getNodeMap().iterator();
        while (nodeIterator.hasNext()) {
            var node = nodeIterator.next();
            var nodeId = node.getEntityId();

            // We have R random walks for each node in the graph
            for (int i = 0; i < randomWalks; i++) {
                var walk = LongLists.mutable.of(nodeId);

                updateWalksState(nodeId, walkIndex);

                createRoute(nodeId, walk, walkIndex, epsilon);

                walks.add(walk);
                walkIndex++;
            }
        }

        return calculatePageRank();
    }

    private void consumeUpdate(InMemoryEntity update) {
        if (update instanceof InMemoryNode node) {
            graph.updateNode(node);
            if (!node.isDeleted()) {
                addNode(node);
            } else {
                deleteNode(node);
            }
        } else if (update instanceof InMemoryRelationship rel) {
            graph.updateRelationship(rel);
            if (!rel.isDeleted()) {
                addRelationship(rel);
            } else {
                deleteRelationship(rel);
            }
        } else {
            throw new IllegalArgumentException(String.format("Invalid update type %s", update));
        }
    }

    private void addNode(InMemoryNode node) {
        var walkIndex = walks.size();
        for (int i = 0; i < randomWalks; ++i) {
            var nodeId = node.getEntityId();
            var walk = LongLists.mutable.of(nodeId);

            updateWalksState(nodeId, walkIndex);

            createRoute(nodeId, walk, walkIndex, epsilon);

            walks.add(walk);
            walkIndex++;
        }
    }

    private void deleteNode(InMemoryNode node) {
        walksTable.remove(node.getEntityId());
        walksCounter.remove(node.getEntityId());
    }

    private void addRelationship(InMemoryRelationship rel) {
        var from = rel.getStartNode();
        var prevWalksTable = walksTable.getOrDefault(from, new LongHashSet());

        var walksTableCopy = new LongHashSet(prevWalksTable);
        walksTableCopy.forEach(walkIndex -> {
            var walk = walks.get(Math.toIntExact(walkIndex));

            var position = walk.indexOf(from) + 1;
            if (position != 0) { // from was found
                var i = position;
                while (i != walk.size()) {
                    var nodeId = walk.get(i);
                    walksTable.get(nodeId).remove(walkIndex);
                    walksCounter.put(nodeId, walksCounter.get(nodeId) - 1);
                    i++;
                }
                if (position < walk.size()) {
                    // todo: implement sublist
                    // walk.subList(position, walk.size()).clear();
                    var size = walk.size();
                    for (int p = position; p < size; ++p) {
                        walk.removeAtIndex(walk.size() - 1);
                    }
                }
            }

            var halfEps = epsilon / 2.0;
            createRoute(from, walk, walkIndex, halfEps);
        });
    }

    private void deleteRelationship(InMemoryRelationship rel) {
        var from = rel.getStartNode();
        var prevWalksTable = walksTable.getOrDefault(from, new LongHashSet());

        var walksTableCopy = new LongHashSet(prevWalksTable);
        walksTableCopy.forEach(walkIndex -> {
            var walk = walks.get(Math.toIntExact(walkIndex));

            var position = walk.indexOf(from) + 1;
            if (position != 0) { // from was not found
                var i = position;
                while (i != walk.size()) {
                    var nodeId = walk.get(i);
                    if (walksTable.containsKey(nodeId)) {
                        walksTable.get(nodeId).remove(walkIndex);
                        walksCounter.put(nodeId, walksCounter.get(nodeId) - 1);
                    }
                    i++;
                }
                // todo: implement sublist
                // walk.subList(position, walk.size()).clear();
                var size = walk.size();
                for (int p = position; p < size; ++p) {
                    walk.removeAtIndex(walk.size() - 1);
                }

                // Skip creating routes if node does not exist anymore
                if (graph.getNode((int) from).isPresent()) {
                    var halfEps = epsilon / 2.0;
                    createRoute(from, walk, walkIndex, halfEps);
                }
            }
        });
    }
}
