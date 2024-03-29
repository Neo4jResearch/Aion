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
package org.neo4j.index.internal.gbptree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.TreeState.read;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCursor;

class TreeStateTest {
    private static final int PAGE_SIZE = 256;
    private PageAwareByteArrayCursor cursor;

    @BeforeEach
    void initiateCursor() {
        cursor = new PageAwareByteArrayCursor(PAGE_SIZE);
        cursor.next();
    }

    @Test
    void readEmptyStateShouldThrow() throws IOException {
        // GIVEN empty state

        // WHEN
        TreeState state = read(cursor);

        // THEN
        assertFalse(state.isValid());
    }

    @Test
    void shouldReadValidPage() throws IOException {
        // GIVEN valid state
        long pageId = cursor.getCurrentPageId();
        TreeState expected = new TreeState(pageId, 1, 2, 3, 4, 5, 6, 7, 8, 9, true, true);
        write(cursor, expected);
        cursor.setOffset(0);

        // WHEN
        TreeState read = read(cursor);

        // THEN
        assertEquals(expected, read);
    }

    @Test
    void readBrokenStateShouldFail() throws IOException {
        // GIVEN broken state
        long pageId = cursor.getCurrentPageId();
        TreeState expected = new TreeState(pageId, 1, 2, 3, 4, 5, 6, 7, 8, 9, true, true);
        write(cursor, expected);
        cursor.setOffset(0);
        assertTrue(read(cursor).isValid());
        cursor.setOffset(0);
        breakChecksum(cursor);

        // WHEN
        TreeState state = read(cursor);

        // THEN
        assertFalse(state.isValid());
    }

    @Test
    void shouldNotWriteInvalidStableGeneration() {
        long generation = GenerationSafePointer.MAX_GENERATION + 1;

        assertThrows(IllegalArgumentException.class, () -> {
            long pageId = cursor.getCurrentPageId();
            write(cursor, new TreeState(pageId, generation, 2, 3, 4, 5, 6, 7, 8, 9, true, true));
        });
    }

    @Test
    void shouldNotWriteInvalidUnstableGeneration() {
        long generation = GenerationSafePointer.MAX_GENERATION + 1;

        assertThrows(IllegalArgumentException.class, () -> {
            long pageId = cursor.getCurrentPageId();
            write(cursor, new TreeState(pageId, 1, generation, 3, 4, 5, 6, 7, 8, 9, true, true));
        });
    }

    private static void breakChecksum(PageCursor cursor) {
        // Doesn't matter which bits we destroy actually. Destroying the first ones requires
        // no additional knowledge about where checksum is stored
        long existing = cursor.getLong(cursor.getOffset());
        cursor.putLong(cursor.getOffset(), ~existing);
    }

    private static void write(PageCursor cursor, TreeState origin) {
        TreeState.write(
                cursor,
                origin.stableGeneration(),
                origin.unstableGeneration(),
                origin.rootId(),
                origin.rootGeneration(),
                origin.lastId(),
                origin.freeListWritePageId(),
                origin.freeListReadPageId(),
                origin.freeListWritePos(),
                origin.freeListReadPos(),
                origin.isClean());
    }
}
