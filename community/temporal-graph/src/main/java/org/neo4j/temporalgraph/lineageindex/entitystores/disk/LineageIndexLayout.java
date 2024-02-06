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
import org.apache.commons.lang3.tuple.MutablePair;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

public class LineageIndexLayout extends Layout.Adapter<MutablePair<Long, Long>, BufferedBytes> {
    private static final boolean FIXED_SIZE = false;
    private static final long ID = 903;
    private static final int MAJOR_VERSION = -1;
    private static final int MINOR_VERSION = -1;

    public LineageIndexLayout() {
        super(FIXED_SIZE, ID, MAJOR_VERSION, MINOR_VERSION);
    }

    @Override
    public int compare(MutablePair<Long, Long> o1, MutablePair<Long, Long> o2) {
        return o1.compareTo(o2);
    }

    @Override
    public MutablePair<Long, Long> newKey() {
        return new MutablePair<>();
    }

    @Override
    public MutablePair<Long, Long> copyKey(MutablePair<Long, Long> key, MutablePair<Long, Long> into) {
        into.setLeft(key.getLeft());
        into.setRight(key.getRight());
        return into;
    }

    @Override
    public BufferedBytes newValue() {
        return new BufferedBytes(128);
    }

    @Override
    public int keySize(MutablePair<Long, Long> key) {
        return 2 * Long.BYTES;
    }

    @Override
    public int valueSize(BufferedBytes value) {
        if (value == null) {
            return -1;
        }
        return value.limit;
    }

    @Override
    public void writeKey(PageCursor cursor, MutablePair<Long, Long> key) {
        cursor.putLong(key.getLeft());
        cursor.putLong(key.getRight());
    }

    @Override
    public void writeValue(PageCursor cursor, BufferedBytes value) {
        cursor.putBytes(value.buffer.array(), 0, value.limit);
    }

    @Override
    public void readKey(PageCursor cursor, MutablePair<Long, Long> into, int keySize) {
        into.setLeft(cursor.getLong());
        into.setRight(cursor.getLong());
    }

    @Override
    public void initializeAsLowest(MutablePair<Long, Long> key) {
        key.setLeft(Long.MIN_VALUE);
        key.setRight(Long.MIN_VALUE);
    }

    @Override
    public void initializeAsHighest(MutablePair<Long, Long> key) {
        key.setLeft(Long.MAX_VALUE);
        key.setRight(Long.MAX_VALUE);
    }

    @Override
    public void readValue(PageCursor cursor, BufferedBytes into, int valueSize) {
        if (into.buffer.capacity() < valueSize) {
            into.buffer = ByteBuffer.allocate(valueSize);
        }
        into.buffer.position(0);
        cursor.getBytes(into.buffer.array(), 0, valueSize);
        into.buffer.limit(valueSize);
        into.limit = valueSize;
    }
}
