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
package org.neo4j.kernel.impl.index.schema;

import java.util.function.Consumer;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Writes index state in the {@link GBPTree} header.
 */
public class NativeIndexHeaderWriter implements Consumer<PageCursor> {
    private final byte state;

    public NativeIndexHeaderWriter(byte state) {
        this.state = state;
    }

    @Override
    public void accept(PageCursor cursor) {
        cursor.putByte(state);
    }
}
