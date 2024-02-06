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
package org.neo4j.temporalgraph.timeindex.timestore.disk;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.MutablePair;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

public class TimeIndexLayout extends Layout.Adapter<MutablePair<Long, Long>, MutableInt> {
    private static final boolean FIXED_SIZE = true;
    private static final long ID = 902;
    private static final int MAJOR_VERSION = -1;
    private static final int MINOR_VERSION = -1;

    public TimeIndexLayout() {
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
    public MutableInt newValue() {
        return new MutableInt();
    }

    @Override
    public int keySize(MutablePair<Long, Long> key) {
        return 2 * Long.BYTES;
    }

    @Override
    public int valueSize(MutableInt value) {
        return Integer.BYTES;
    }

    @Override
    public void writeKey(PageCursor cursor, MutablePair<Long, Long> key) {
        cursor.putLong(key.getLeft());
        cursor.putLong(key.getRight());
    }

    @Override
    public void writeValue(PageCursor cursor, MutableInt value) {
        cursor.putInt(value.intValue());
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
    public void readValue(PageCursor cursor, MutableInt into, int valueSize) {
        into.setValue(cursor.getInt());
    }
}
