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
package org.neo4j.temporalprocs.result;

public class NodeSimilarityResult {
    public final long timestamp;
    public final long nodeId1;
    public final long nodeId2;
    public final double similarity;

    public NodeSimilarityResult(long timestamp, long nodeId1, long nodeId2, double similarity) {
        this.timestamp = timestamp;
        this.nodeId1 = nodeId1;
        this.nodeId2 = nodeId2;
        this.similarity = similarity;
    }
}
