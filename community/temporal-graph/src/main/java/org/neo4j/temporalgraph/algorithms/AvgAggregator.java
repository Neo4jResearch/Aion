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

public class AvgAggregator implements Aggregator {
    private double sum;
    private double count;
    private final double neutralVal;

    public AvgAggregator() {
        sum = 0;
        count = 0;
        neutralVal = 0;
    }

    @Override
    public void insert(double value) {
        sum += value;
        count++;
    }

    @Override
    public boolean evict(double value) {
        sum -= value;
        count--;
        return true;
    }

    @Override
    public double query() {
        return count != 0 ? sum / count : neutralVal;
    }

    @Override
    public void reset() {
        sum = 0;
        count = 0;
    }
}
