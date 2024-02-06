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
package org.neo4j.temporalgraph.utils;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ThreadPool {
    public static final int THREAD_NUMBER = 8;
    static final ExecutorService executor = Executors.newFixedThreadPool(THREAD_NUMBER);

    private ThreadPool() {}

    private static class SingletonHelper {
        private static final ThreadPool INSTANCE = new ThreadPool();
    }

    public static ThreadPool getInstance() {
        return SingletonHelper.INSTANCE;
    }

    public Future<Boolean> submit(Callable<Boolean> callable) {
        return executor.submit(callable);
    }

    public <T> List<Future<T>> invokeAll(List<Callable<T>> callableList) throws InterruptedException {
        return executor.invokeAll(callableList);
    }

    public void shutdown() {
        executor.shutdown();
    }
}
