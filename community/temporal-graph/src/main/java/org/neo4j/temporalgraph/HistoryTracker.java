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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.server.rest.repr.InvalidArgumentsException;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.neo4j.temporalgraph.entities.PropertyType;

public abstract class HistoryTracker extends TransactionEventListenerAdapter<Object> {
    protected final Map<String, Integer> namesToIds; // todo: must be loaded from disk upon start
    protected final Map<Integer, String> idsToNames;
    protected final Set<String> nodeLabelNames;
    protected final Set<String> nodePropertyNames;
    protected final Set<String> relTypeNames;
    protected final Set<String> relPropertyNames;
    protected final Map<String, PropertyType> relPropertyNamesToTypes;
    protected long lastTransactionId;
    protected long lastCommittedTime;

    // Thread local variables for graph retrieval
    private static final ThreadLocal<Map<Long, InMemoryNode>> nodeMap = ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<Map<Long, InMemoryRelationship>> relMap = ThreadLocal.withInitial(HashMap::new);

    protected HistoryTracker() {
        namesToIds = new ConcurrentHashMap<>();
        idsToNames = new ConcurrentHashMap<>();
        nodeLabelNames = new ConcurrentSkipListSet<>();
        nodePropertyNames = new ConcurrentSkipListSet<>();
        relTypeNames = new ConcurrentSkipListSet<>();
        relPropertyNames = new ConcurrentSkipListSet<>();
        relPropertyNamesToTypes = new ConcurrentHashMap<>();
        lastTransactionId = -1L;
        lastCommittedTime = -1L;

        // Hack to run gds without transactional inserts
        // namesToIds.putIfAbsent("CONNECTED", 0);
        // idsToNames.putIfAbsent(0, "CONNECTED");
        // relTypeNames.add("CONNECTED");
    }

    @Override
    public Object beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
        // Get the encoding of node properties
        if (data.assignedNodeProperties().iterator().hasNext()) {
            for (var p : data.assignedNodeProperties()) {
                if (!namesToIds.containsKey(p.key())) {
                    var id = ((NodeEntity) p.entity()).getPropertyKey(p.key());
                    namesToIds.putIfAbsent(p.key(), id);
                    idsToNames.putIfAbsent(id, p.key());
                    nodePropertyNames.add(p.key());
                }
            }
        }

        // Get the encoding of node labels
        if (data.assignedLabels().iterator().hasNext()) {
            for (var p : data.assignedLabels()) {
                if (!namesToIds.containsKey(p.label().name())) {
                    var id = ((NodeEntity) p.node()).getLabelId(p.label().name());
                    namesToIds.putIfAbsent(p.label().name(), id);
                    idsToNames.putIfAbsent(id, p.label().name());
                    nodeLabelNames.add(p.label().name());
                }
            }
        }

        // Get encoding for the type of added relationships
        if (data.createdRelationships().iterator().hasNext()) {
            for (var rel : data.createdRelationships()) {
                var typeName = rel.getType().name();
                if (!namesToIds.containsKey(typeName)) {
                    var id = ((RelationshipEntity) rel).typeId();
                    namesToIds.putIfAbsent(typeName, id);
                    idsToNames.putIfAbsent(id, typeName);
                    relTypeNames.add(typeName);
                }
            }
        }

        // Get the encoding of relationship properties
        if (data.assignedRelationshipProperties().iterator().hasNext()) {
            for (var p : data.assignedRelationshipProperties()) {
                if (!namesToIds.containsKey(p.key())) {
                    var id = ((RelationshipEntity) p.entity()).getPropertyKey(p.key());
                    namesToIds.putIfAbsent(p.key(), id);
                    idsToNames.putIfAbsent(id, p.key());
                    relPropertyNames.add(p.key());

                    // Track the property types of relationships
                    var propertyType = PropertyType.getType(p.value());
                    if (!relPropertyNamesToTypes.containsKey(p.key())) {
                        relPropertyNamesToTypes.put(p.key().trim(), propertyType);
                    } else {
                        if (relPropertyNamesToTypes.get(p.key()) != propertyType) {
                            throw new IllegalStateException(
                                    String.format("The previous property type was not %s", propertyType.toString()));
                        }
                    }
                }
            }
        }

        return null;
    }

    public long getLastCommittedTime() {
        return lastCommittedTime;
    }

    public long getLastTransactionId() {
        return lastTransactionId;
    }

    public int getIdFromName(String name) throws InvalidArgumentsException {
        var id = namesToIds.get(name);
        if (id == null) {
            throw new InvalidArgumentsException(String.format("Invalid name: %s", name));
        }
        return id;
    }

    public List<String> getNodeLabelNames() {
        return new ArrayList<>(nodeLabelNames);
    }

    public List<String> getNodePropertyNames() {
        return new ArrayList<>(nodePropertyNames);
    }

    public List<String> getRelationshipTypeNames() {
        return new ArrayList<>(relTypeNames);
    }

    public List<String> getRelationshipPropertyNames() {
        return new ArrayList<>(relPropertyNames);
    }

    public Map<String, PropertyType> getRelPropertyNamesToTypes() {
        return new HashMap<>(relPropertyNamesToTypes);
    }

    public void reset() {
        lastTransactionId = -1L;
        lastCommittedTime = -1L;
        namesToIds.clear();
        idsToNames.clear();
    }

    protected long getTimestamp(TransactionData data) {
        return data.getCommitTime();
    }

    protected long getTransactionId(TransactionData data) {
        return data.getTransactionId();
    }

    protected List<InMemoryNode> getNodes(TransactionData data, long timestamp) {
        var map = nodeMap.get();
        map.clear();

        // First, get the nodes updated
        for (Node n : data.createdNodes()) {
            map.put(n.getId(), new InMemoryNode(n.getId(), timestamp));
        }
        // Then, add the new properties/labels
        for (var p : data.assignedNodeProperties()) {
            var nodeId = p.entity().getId();
            var node = map.get(nodeId);
            if (node == null) {
                node = new InMemoryNode(nodeId, timestamp, false, true);
                map.put(nodeId, node);
            }
            node.addProperty(p.key(), p.value());
        }
        for (var l : data.assignedLabels()) {
            var nodeId = l.node().getId();
            var node = map.get(nodeId);
            if (node == null) {
                node = new InMemoryNode(nodeId, timestamp, false, true);
                map.put(nodeId, node);
            }
            node.addLabel(l.label().name());
        }

        // Next, we handle deletions starting from node deletions
        for (Node n : data.deletedNodes()) {
            map.put(n.getId(), new InMemoryNode(n.getId(), timestamp, true, false));
        }
        // Then, we delete properties/labels
        for (var p : data.removedNodeProperties()) {
            var nodeId = p.entity().getId();
            var node = map.get(nodeId);
            if (node == null) {
                node = new InMemoryNode(nodeId, timestamp, false, true);
                map.put(nodeId, node);
            }
            node.removeProperty(p.key());
        }
        for (var l : data.removedLabels()) {
            var nodeId = l.node().getId();
            var node = map.get(nodeId);
            if (node == null) {
                node = new InMemoryNode(nodeId, timestamp, false, true);
                map.put(nodeId, node);
            }
            node.removeLabel(l.label().name());
        }

        return new ArrayList<>(map.values());
    }

    protected List<InMemoryRelationship> getRelationships(TransactionData data, long timestamp) {
        var map = relMap.get();
        map.clear();

        // First, get the relationships updated
        for (var r : data.createdRelationships()) {
            map.put(r.getId(), createRelationship(r, timestamp));
        }
        // Then, add the new properties/labels
        for (var p : data.assignedRelationshipProperties()) {
            var relId = p.entity().getId();
            var rel = map.get(relId);
            if (rel == null) {
                rel = createRelationship(p.entity(), timestamp);
                rel.setDiff();
                map.put(relId, rel);
            }
            rel.addProperty(p.key(), p.value());
        }

        // Next, we handle deletions starting from edge deletions
        for (var r : data.deletedRelationships()) {
            var rel = createRelationship(r, timestamp);
            rel.setDeleted();
            map.put(r.getId(), rel);
        }
        // Then, we delete properties
        for (var p : data.removedRelationshipProperties()) {

            var relId = p.entity().getId();
            var rel = map.get(relId);
            if (rel == null) {
                rel = createRelationship(p.entity(), timestamp);
                rel.setDiff();
                map.put(relId, rel);
            }
            rel.removeProperty(p.key());
        }

        return new ArrayList<>(map.values());
    }

    protected InMemoryRelationship createRelationship(Relationship r, long timestamp) {
        return new InMemoryRelationship(
                r.getId(), r.getStartNodeId(), r.getEndNodeId(), ((RelationshipEntity) r).typeId(), timestamp);
    }

    public abstract void shutdown() throws IOException;
}
