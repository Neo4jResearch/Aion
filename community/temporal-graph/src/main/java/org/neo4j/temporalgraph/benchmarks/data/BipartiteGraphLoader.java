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
import java.util.HashMap;
import java.util.Map;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;

public abstract class BipartiteGraphLoader extends GraphLoader {
    protected static final boolean SHUFFLE = false;

    protected BipartiteGraphLoader(String stringPath, String name) {
        super(stringPath, name);
    }

    @Override
    public void loadGraphFromDisk() throws IOException {
        if (!Path.of(stringPath).toFile().exists()) {
            System.err.printf("input file (%s) does not exist!%n", stringPath);
        }

        long counter = 0;
        int maxNodeId = Integer.MIN_VALUE;
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

                // Keep track of maximum id so far
                maxNodeId = Math.max(maxNodeId, from);

                // Create one relationship
                var forwardRel = new InMemoryRelationship(counter, from, to, 0, counter);
                relationships.add(forwardRel);
                counter++;
            }
        }

        relabelTargetIds(maxNodeId);
        if (SHUFFLE) {
            shuffleRelationships();
        }
    }

    protected void relabelTargetIds(int maxNodeId) {
        var currentId = maxNodeId + 1;
        Map<Integer, Integer> map = new HashMap<>();
        for (var r : relationships) {
            var originalId = (int) r.getEndNode();
            var id = -1;
            if (map.containsKey(originalId)) {
                id = map.get(originalId);
            } else {
                id = currentId;
                map.put(originalId, id);
                nodeSet.add(id);

                currentId++;
            }
            r.setEndNode(id);
        }
    }
}
