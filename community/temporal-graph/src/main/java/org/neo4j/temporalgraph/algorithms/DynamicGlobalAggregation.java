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

import java.util.List;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryGraph;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;

public class DynamicGlobalAggregation implements DynamicAlgorithm<Double> {
    private InMemoryGraph graph; // todo: the graph should be updated externally
    private final Aggregator aggr;
    private final String propertyName;

    public DynamicGlobalAggregation(Aggregator aggr, String propertyName) {
        this.aggr = aggr;
        this.propertyName = propertyName;
    }

    @Override
    public void initialize(InMemoryGraph graph) {
        this.graph = graph;
        initializeAggregateStore();
    }

    /*
       Treat entity updates as a deletion followed by an insertion.
    */
    @Override
    public void update(List<InMemoryEntity> graphUpdates) {
        for (var update : graphUpdates) {
            if (update instanceof InMemoryNode node) {
                InMemoryNode prevNode = null;
                if (node.isDeleted() || node.isDiff()) {
                    var optionalPrevNode = graph.getNode((int) node.getEntityId());
                    assert (optionalPrevNode.isPresent());
                    prevNode = optionalPrevNode.get();
                    removeFromAggregationStore(prevNode);
                    if (node.isDiff()) {
                        prevNode.merge(node);
                    }
                }
                if (!node.isDeleted()) {
                    insertInAggregationStore(node.isDiff() ? prevNode : node);
                }
                graph.updateNode(node);
            } else if (update instanceof InMemoryRelationship rel) {
                InMemoryRelationship prevRel = null;
                if (rel.isDeleted() || rel.isDiff()) {
                    var optionalPrevRel = graph.getRelationship((int) rel.getEntityId());
                    assert (optionalPrevRel.isPresent());
                    prevRel = optionalPrevRel.get();
                    removeFromAggregationStore(prevRel);
                    if (rel.isDiff()) {
                        prevRel.merge(rel);
                    }
                }
                if (!rel.isDeleted()) {
                    insertInAggregationStore(rel.isDiff() ? prevRel : rel);
                }
                graph.updateRelationship(rel);
            } else {
                throw new IllegalArgumentException(String.format("Invalid update type %s", update));
            }
        }
    }

    @Override
    public Double getResult() {
        if (aggr == null) {
            throw new IllegalStateException("Aggregation is not computed yet");
        }
        return aggr.query();
    }

    @Override
    public void reset() {
        graph = null;
        aggr.reset();
    }

    private void insertInAggregationStore(InMemoryEntity e) {
        for (var p : e.getProperties()) {
            if (p.name().equals(propertyName)) {
                aggr.insert(getValue(p.value()));
            }
        }
    }

    private void removeFromAggregationStore(InMemoryEntity e) {
        for (var p : e.getProperties()) {
            if (p.name().equals(propertyName)) {
                aggr.evict(getValue(p.value()));
            }
        }
    }

    private void initializeAggregateStore() {
        aggr.reset();
        var nodeIterator = graph.getNodeMap().iterator();
        while (nodeIterator.hasNext()) {
            var n = nodeIterator.next();
            insertInAggregationStore(n);
        }

        var relIterator = graph.getRelationshipMap().iterator();
        while (relIterator.hasNext()) {
            var r = relIterator.next();
            insertInAggregationStore(r);
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
            throw new IllegalStateException(String.format("Unsupported type %s", obj.toString()));
        }
    }
}
