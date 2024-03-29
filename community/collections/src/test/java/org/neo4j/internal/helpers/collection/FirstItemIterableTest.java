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
package org.neo4j.internal.helpers.collection;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class FirstItemIterableTest {
    @Test
    void testEmptyIterator() {
        FirstItemIterable<?> firstItemIterable = new FirstItemIterable<>(Collections.emptyList());
        Iterator<?> empty = firstItemIterable.iterator();
        assertFalse(empty.hasNext());
        assertThrows(NoSuchElementException.class, empty::next);
        assertNull(firstItemIterable.getFirst());
    }

    @Test
    void testSingleIterator() {
        FirstItemIterable<Boolean> firstItemIterable = new FirstItemIterable<>(Collections.singleton(Boolean.TRUE));
        Iterator<Boolean> empty = firstItemIterable.iterator();
        assertTrue(empty.hasNext());
        assertEquals(Boolean.TRUE, empty.next());
        assertEquals(Boolean.TRUE, firstItemIterable.getFirst());
        assertFalse(empty.hasNext());
        assertThrows(NoSuchElementException.class, empty::next);
        assertEquals(Boolean.TRUE, firstItemIterable.getFirst());
    }

    @Test
    void testMultiIterator() {
        FirstItemIterable<Boolean> firstItemIterable = new FirstItemIterable<>(asList(Boolean.TRUE, Boolean.FALSE));
        Iterator<Boolean> empty = firstItemIterable.iterator();
        assertTrue(empty.hasNext());
        assertEquals(Boolean.TRUE, empty.next());
        assertEquals(Boolean.TRUE, firstItemIterable.getFirst());
        assertTrue(empty.hasNext());
        assertEquals(Boolean.FALSE, empty.next());
        assertEquals(Boolean.TRUE, firstItemIterable.getFirst());
        assertFalse(empty.hasNext());
        assertThrows(NoSuchElementException.class, empty::next);
        assertEquals(Boolean.TRUE, firstItemIterable.getFirst());
    }
}
