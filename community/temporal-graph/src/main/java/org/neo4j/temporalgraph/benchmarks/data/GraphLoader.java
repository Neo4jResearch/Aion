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
package org.neo4j.temporalgraph.benchmarks.data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.eclipse.collections.impl.factory.Lists;
import org.neo4j.temporalgraph.benchmarks.TemporalStore;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;
import org.roaringbitmap.RoaringBitmap;

public abstract class GraphLoader {
    protected final String name;
    protected final String stringPath;
    protected final RoaringBitmap nodeSet;
    protected final List<InMemoryRelationship> relationships;
    protected final boolean shuffle = true;
    public static final String DATASET_FOLDER =
            "/Users/georgetheodorakis/Documents/datasets"; // "/home/ec2-user/datasets";

    protected GraphLoader(String stringPath, String name) {
        this.stringPath = stringPath;
        this.name = name;
        this.relationships = Lists.mutable.empty();
        this.nodeSet = new RoaringBitmap();
    }

    public String getDatasetName() {
        return name;
    }

    public void loadGraphFromDisk() throws IOException {
        if (!Path.of(stringPath).toFile().exists()) {
            System.err.printf("input file (%s) does not exist!%n", stringPath);
        }

        long counter = 0;
        try (var file = new BufferedReader(new FileReader(stringPath))) {
            String string;
            while ((string = file.readLine()) != null) {
                if (string.startsWith("%") || string.startsWith("#")) {
                    continue;
                }
                String[] nodeIds = string.split("\\s+");
                assert (nodeIds.length == 2);
                var from = Integer.parseInt(nodeIds[0]);
                var to = Integer.parseInt(nodeIds[1]);
                nodeSet.add(from);
                nodeSet.add(to);

                // Create one relationship
                var forwardRel = new InMemoryRelationship(counter, from, to, 42, counter);
                relationships.add(forwardRel);
                counter++;
            }
        }
        shuffleRelationships();
    }

    protected void shuffleRelationships() {
        if (shuffle) {
            Collections.shuffle(relationships, new Random(42L));
            long counter = 0;
            for (var r : relationships) {
                r.setEntityId(counter);
                r.setStartTimestamp(counter);
                counter++;
            }
        }
    }

    public void importGraphToTemporalStore(TemporalStore store) throws IOException {
        // Construct and add nodes
        List<InMemoryNode> nodes = Lists.mutable.empty();
        for (var nodeId : nodeSet) {
            nodes.add(new InMemoryNode(nodeId, 0));
        }
        store.addNodes(nodes);
        nodes.clear();

        // Add relationships
        store.addRelationships(relationships);
    }

    public int numberOfNodes() {
        return nodeSet.getCardinality();
    }

    public int numberOfRelationships() {
        return relationships.size();
    }

    public RoaringBitmap getNodes() {
        return nodeSet;
    }

    public List<InMemoryRelationship> getRelationships() {
        return relationships;
    }

    public void clear() {
        nodeSet.clear();
        relationships.clear();
    }
}
