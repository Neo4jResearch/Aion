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

import java.nio.file.Path;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import org.neo4j.index.internal.gbptree.MultiRootGBPTree.Monitor;

class GBPTreeCleanupJob implements CleanupJob {
    private final CrashGenerationCleaner crashGenerationCleaner;
    private final CountDownLatch lock;
    private final Monitor monitor;
    private final Path indexFile;
    private volatile boolean needed;
    private volatile Throwable failure;

    /**
     * @param crashGenerationCleaner {@link CrashGenerationCleaner} to use for cleaning.
     * @param lock {@link LongSpinLatch} to be released when job has either successfully finished or failed.
     * @param monitor {@link Monitor} to report to
     * @param indexFile Target file
     */
    GBPTreeCleanupJob(
            CrashGenerationCleaner crashGenerationCleaner, CountDownLatch lock, Monitor monitor, Path indexFile) {
        this.crashGenerationCleaner = crashGenerationCleaner;
        this.lock = lock;
        this.monitor = monitor;
        this.indexFile = indexFile;
        this.needed = true;
    }

    @Override
    public boolean needed() {
        return needed;
    }

    @Override
    public boolean hasFailed() {
        return failure != null;
    }

    @Override
    public Throwable getCause() {
        return failure;
    }

    @Override
    public void close() {
        lock.countDown();
        monitor.cleanupClosed();
    }

    @Override
    public void run(Executor executor) {
        try {
            crashGenerationCleaner.clean(executor);
            needed = false;
        } catch (Throwable e) {
            monitor.cleanupFailed(e);
            failure = e;
        }
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", "CleanupJob(", ")");
        joiner.add("file=" + indexFile.toAbsolutePath());
        joiner.add("needed=" + needed);
        joiner.add("failure=" + failure);
        return joiner.toString();
    }
}
