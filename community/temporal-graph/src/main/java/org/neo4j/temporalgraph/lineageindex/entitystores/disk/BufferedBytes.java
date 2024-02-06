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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.StringJoiner;

public class BufferedBytes {
    static final BufferedBytes EMPTY_BYTES = new BufferedBytes(0);

    int limit;
    ByteBuffer buffer;

    public BufferedBytes(int capacity) {
        limit = 0;
        buffer = ByteBuffer.allocate(capacity);
    }

    public void setLimit(int limit) {
        if ((buffer == null || limit >= buffer.capacity())) {
            throw new IllegalArgumentException(String.format("Invalid limit %d", limit));
        }
        this.limit = limit;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        int nbrOfAccumulatedZeroes = 0;
        for (int i = 0; i < limit; ++i) {
            var b = buffer.get(i);
            if (b != (byte) 0) {
                if (nbrOfAccumulatedZeroes > 0) {
                    joiner.add(replaceZeroes(nbrOfAccumulatedZeroes));
                    nbrOfAccumulatedZeroes = 0;
                }
                joiner.add(Byte.toString(b));
            } else {
                nbrOfAccumulatedZeroes++;
            }
        }
        if (nbrOfAccumulatedZeroes > 0) {
            joiner.add(replaceZeroes(nbrOfAccumulatedZeroes));
        }
        return joiner.toString();
    }

    private static String replaceZeroes(int nbrOfZeroes) {
        if (nbrOfZeroes > 3) {
            return "0...>" + nbrOfZeroes;
        } else {
            StringJoiner joiner = new StringJoiner(", ");
            for (int i = 0; i < nbrOfZeroes; i++) {
                joiner.add("0");
            }
            return joiner.toString();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BufferedBytes other = (BufferedBytes) o;
        return limit == other.limit && Arrays.equals(buffer.array(), other.buffer.array());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(buffer.array());
    }
}
