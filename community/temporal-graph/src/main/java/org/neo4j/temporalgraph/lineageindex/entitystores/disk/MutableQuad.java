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
package org.neo4j.temporalgraph.lineageindex.entitystores.disk;

import java.io.Serializable;
import java.util.Objects;
import org.apache.commons.lang3.builder.CompareToBuilder;

public class MutableQuad<T1, T2, T3, T4> implements Comparable<MutableQuad<T1, T2, T3, T4>>, Serializable {

    private static final long serialVersionUID = 1L;

    public T1 first;
    public T2 second;
    public T3 third;
    public T4 fourth;

    public MutableQuad() {}

    public MutableQuad(final T1 first, final T2 second, final T3 third, final T4 fourth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
    }

    public T1 getFirst() {
        return first;
    }

    public T2 getSecond() {
        return second;
    }

    public T3 getThird() {
        return third;
    }

    public T4 getFourth() {
        return fourth;
    }

    public void setFirst(T1 obj) {
        first = obj;
    }

    public void setSecond(T2 obj) {
        second = obj;
    }

    public void setThird(T3 obj) {
        third = obj;
    }

    public void setFourth(T4 obj) {
        fourth = obj;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof MutableQuad<?, ?, ?, ?> other) {
            return Objects.equals(getFirst(), other.getFirst())
                    && Objects.equals(getSecond(), other.getSecond())
                    && Objects.equals(getThird(), other.getThird())
                    && Objects.equals(getFourth(), other.getFourth());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getFirst())
                ^ Objects.hashCode(getSecond())
                ^ Objects.hashCode(getThird())
                ^ Objects.hashCode(getFourth());
    }

    @Override
    public String toString() {
        return "(" + getFirst() + "," + getSecond() + "," + getThird() + "," + getFourth() + ")";
    }

    @Override
    public int compareTo(MutableQuad<T1, T2, T3, T4> other) {
        return new CompareToBuilder()
                .append(getFirst(), other.getFirst())
                .append(getSecond(), other.getSecond())
                .append(getThird(), other.getThird())
                .append(getFourth(), other.getFourth())
                .toComparison();
    }
}
