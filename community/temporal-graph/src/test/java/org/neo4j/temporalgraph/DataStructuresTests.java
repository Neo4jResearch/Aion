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
package org.neo4j.temporalgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.temporalgraph.utils.IntCircularList;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;

@ExtendWith({TestDirectorySupportExtension.class})
class DataStructuresTests {

    @Test
    void shouldEnqueueData() {
        final int batchSize = 1_000_000;
        Queue<Integer> queue = new LinkedList<>();
        IntCircularList testQueue = new IntCircularList();

        // Test enqueue and dequeue
        Random random = new Random(42L);
        for (var iteration = 0; iteration < 5; iteration++) {
            for (int i = 0; i < batchSize; ++i) {
                var e = random.nextInt();
                queue.add(e);
                testQueue.add(e);
            }
            while (!testQueue.isEmpty()) {
                assertEquals(queue.poll(), testQueue.poll());
            }
        }
    }

    @Test
    void shouldInitializeFromCollection() {
        final int batchSize = 1_000_000;
        Queue<Integer> queue = new LinkedList<>();
        IntCircularList testQueue = new IntCircularList();

        // Test initialization from a collection
        Random random = new Random(42L);
        for (int i = 0; i < batchSize; ++i) {
            var e = random.nextInt();
            queue.add(e);
        }
        testQueue = new IntCircularList(queue);

        while (!testQueue.isEmpty()) {
            assertEquals(queue.poll(), testQueue.poll());
        }
    }

    @Test
    void shouldClearQueue() {
        final int batchSize = 10;
        IntCircularList testQueue = new IntCircularList();

        for (int i = 0; i < batchSize; ++i) {
            testQueue.add(i);
        }

        // Test clear
        testQueue.clear();
        assertTrue(testQueue.isEmpty());
    }
}
