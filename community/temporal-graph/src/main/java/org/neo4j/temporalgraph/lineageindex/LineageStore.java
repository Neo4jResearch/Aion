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
package org.neo4j.temporalgraph.lineageindex;

import java.io.IOException;
import java.util.List;
import org.neo4j.temporalgraph.HistoryStore;
import org.neo4j.temporalgraph.entities.InMemoryNode;
import org.neo4j.temporalgraph.entities.InMemoryRelationship;

public interface LineageStore extends HistoryStore {
    void addNodes(List<InMemoryNode> nodes) throws IOException;

    void addRelationships(List<InMemoryRelationship> rels) throws IOException;

    List<InMemoryNode> getAllNodes(long timestamp) throws IOException;

    List<InMemoryRelationship> getAllRelationships(long timestamp) throws IOException;

    void flushIndexes();

    void reset();
}
