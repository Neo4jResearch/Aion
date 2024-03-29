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
package org.neo4j.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class FuturesTest {
    private static final Runnable NOOP = () -> {};

    @Test
    void combinedFutureShouldGetResultsAfterAllComplete() throws Exception {
        FutureTask<String> task1 = new FutureTask<>(NOOP, "1");
        FutureTask<String> task2 = new FutureTask<>(NOOP, "2");
        FutureTask<String> task3 = new FutureTask<>(NOOP, "3");

        Future<List<String>> combined = Futures.combine(task1, task2, task3);

        assertThrows(TimeoutException.class, () -> combined.get(10, TimeUnit.MILLISECONDS));

        task3.run();
        task2.run();

        assertThrows(TimeoutException.class, () -> combined.get(10, TimeUnit.MILLISECONDS));

        task1.run();

        List<String> result = combined.get();
        assertThat(result).containsExactly("1", "2", "3");
    }
}
