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

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class MinAggregator implements Aggregator {
    private final Deque<Pair<Double, Double>> front;
    private final Deque<Pair<Double, Double>> back;
    private final double neutralVal;

    public MinAggregator() {
        front = new ArrayDeque<>();
        back = new ArrayDeque<>();
        neutralVal = Double.MAX_VALUE;
    }

    @Override
    public void insert(double value) {
        back.push(ImmutablePair.of(value, Math.min(value, getBackTop())));
    }

    @Override
    public boolean evict(double value) {
        if (front.isEmpty()) {
            while (!back.isEmpty()) {
                var prev = back.peek().getRight();
                front.push(ImmutablePair.of(prev, Math.min(prev, getFrontTop())));
                back.pop();
            }
        }
        if (value == front.peek().getLeft()) {
            front.pop();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public double query() {
        return Math.min(getBackTop(), getFrontTop());
    }

    @Override
    public void reset() {
        back.clear();
        front.clear();
    }

    double getBackTop() {
        return back.isEmpty() ? neutralVal : back.peek().getRight();
    }

    double getFrontTop() {
        return front.isEmpty() ? neutralVal : front.peek().getRight();
    }
}
