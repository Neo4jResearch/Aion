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

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

public class NeighbourhoodIndexLayout extends Layout.Adapter<MutableQuad<Long, Long, Long, Long>, NullValue> {
    private static final boolean FIXED_SIZE = true;
    private static final long ID = 904;
    private static final int MAJOR_VERSION = -1;
    private static final int MINOR_VERSION = -1;

    public NeighbourhoodIndexLayout() {
        super(FIXED_SIZE, ID, MAJOR_VERSION, MINOR_VERSION);
    }

    @Override
    public int compare(MutableQuad<Long, Long, Long, Long> o1, MutableQuad<Long, Long, Long, Long> o2) {
        return o1.compareTo(o2);
    }

    @Override
    public MutableQuad<Long, Long, Long, Long> newKey() {
        return new MutableQuad<>();
    }

    @Override
    public MutableQuad<Long, Long, Long, Long> copyKey(
            MutableQuad<Long, Long, Long, Long> key, MutableQuad<Long, Long, Long, Long> into) {
        into.setFirst(key.getFirst());
        into.setSecond(key.getSecond());
        into.setThird(key.getThird());
        into.setFourth(key.getFourth());
        return into;
    }

    @Override
    public NullValue newValue() {
        return NullValue.INSTANCE;
    }

    @Override
    public int keySize(MutableQuad<Long, Long, Long, Long> key) {
        return 4 * Long.BYTES;
    }

    @Override
    public int valueSize(NullValue value) {
        return NullValue.SIZE;
    }

    @Override
    public void writeKey(PageCursor cursor, MutableQuad<Long, Long, Long, Long> key) {
        cursor.putLong(key.getFirst());
        cursor.putLong(key.getSecond());
        cursor.putLong(key.getThird());
        cursor.putLong(key.getFourth());
    }

    @Override
    public void writeValue(PageCursor cursor, NullValue value) {}

    @Override
    public void readKey(PageCursor cursor, MutableQuad<Long, Long, Long, Long> into, int keySize) {
        into.setFirst(cursor.getLong());
        into.setSecond(cursor.getLong());
        into.setThird(cursor.getLong());
        into.setFourth(cursor.getLong());
    }

    @Override
    public void initializeAsLowest(MutableQuad<Long, Long, Long, Long> key) {
        key.setFirst(Long.MIN_VALUE);
        key.setSecond(Long.MIN_VALUE);
        key.setThird(Long.MIN_VALUE);
        key.setFourth(Long.MIN_VALUE);
    }

    @Override
    public void initializeAsHighest(MutableQuad<Long, Long, Long, Long> key) {
        key.setFirst(Long.MAX_VALUE);
        key.setSecond(Long.MAX_VALUE);
        key.setThird(Long.MAX_VALUE);
        key.setFourth(Long.MAX_VALUE);
    }

    @Override
    public void readValue(PageCursor cursor, NullValue into, int valueSize) {}
}
