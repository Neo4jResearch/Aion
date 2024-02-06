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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.temporalgraph.lineageindex.entitystores.disk.NullValue;

public class LongLayout extends Layout.Adapter<MutableLong, NullValue> {

    private static final boolean FIXED_SIZE = true;
    private static final long ID = 905;
    private static final int MAJOR_VERSION = -1;
    private static final int MINOR_VERSION = -1;

    public LongLayout() {
        super(FIXED_SIZE, ID, MAJOR_VERSION, MINOR_VERSION);
    }

    @Override
    public int compare(MutableLong o1, MutableLong o2) {
        return Long.compare(o1.longValue(), o2.longValue());
    }

    @Override
    public MutableLong newKey() {
        return new MutableLong();
    }

    @Override
    public MutableLong copyKey(MutableLong key, MutableLong into) {
        into.setValue(key.longValue());
        return into;
    }

    @Override
    public NullValue newValue() {
        return NullValue.INSTANCE;
    }

    @Override
    public int keySize(MutableLong key) {
        return Long.BYTES;
    }

    @Override
    public int valueSize(NullValue value) {
        return NullValue.SIZE;
    }

    @Override
    public void writeKey(PageCursor cursor, MutableLong key) {
        cursor.putLong(key.longValue());
    }

    @Override
    public void writeValue(PageCursor cursor, NullValue value) {}

    @Override
    public void readKey(PageCursor cursor, MutableLong into, int keySize) {
        into.setValue(cursor.getLong());
    }

    @Override
    public void readValue(PageCursor cursor, NullValue into, int valueSize) {}

    @Override
    public void initializeAsLowest(MutableLong key) {
        key.setValue(Long.MIN_VALUE);
    }

    @Override
    public void initializeAsHighest(MutableLong key) {
        key.setValue(Long.MAX_VALUE);
    }
}
