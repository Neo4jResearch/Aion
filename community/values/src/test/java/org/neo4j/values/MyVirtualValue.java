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
package org.neo4j.values;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Comparator;
import org.neo4j.values.virtual.VirtualValueGroup;

public class MyVirtualValue extends VirtualValue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(MyVirtualValue.class);

    private final int hashCode;

    MyVirtualValue(int hashCode) {
        this.hashCode = hashCode;
    }

    @Override
    public boolean equals(VirtualValue other) {
        return this == other;
    }

    @Override
    public VirtualValueGroup valueGroup() {
        return null;
    }

    @Override
    public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        return 0;
    }

    @Override
    public Comparison unsafeTernaryCompareTo(VirtualValue other, TernaryComparator<AnyValue> comparator) {
        return Comparison.EQUAL;
    }

    @Override
    protected final int computeHashToMemoize() {
        return hashCode;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTypeName() {
        return "MyVirtualValue";
    }

    @Override
    public void writeTo(AnyValueWriter writer) {}

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE;
    }
}
