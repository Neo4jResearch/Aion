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
package org.neo4j.internal.batchimport.staging;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.neo4j.time.Clocks;

/**
 * Supervises a {@link StageExecution} until it is no longer {@link StageExecution#stillExecuting() executing}.
 * Meanwhile it feeds information about the execution to an {@link ExecutionMonitor}.
 */
public class ExecutionSupervisor {
    private final Clock clock;
    private final ExecutionMonitor monitor;

    public ExecutionSupervisor(Clock clock, ExecutionMonitor monitor) {
        this.clock = clock;
        this.monitor = monitor;
    }

    public ExecutionSupervisor(ExecutionMonitor monitor) {
        this(Clocks.systemClock(), monitor);
    }

    /**
     * Supervises {@link StageExecution}, provides continuous information to the {@link ExecutionMonitor}
     * and returns when the execution is done or an error occurs, in which case an exception is thrown.
     *
     * Made synchronized to ensure that only one set of executions take place at any given time
     * and also to make sure the calling thread goes through a memory barrier (useful both before and after execution).
     *
     * @param execution {@link StageExecution} instances to supervise simultaneously.
     */
    public synchronized void supervise(StageExecution execution) {
        long startTime = currentTimeMillis();
        start(execution);

        try {
            while (!execution.awaitCompletion(monitor.checkIntervalMillis(), TimeUnit.MILLISECONDS)) {
                monitor.check(execution);
            }
        } catch (InterruptedException e) {
            execution.panic(e);
        } finally {
            end(execution, currentTimeMillis() - startTime);
        }
    }

    private long currentTimeMillis() {
        return clock.millis();
    }

    protected void end(StageExecution execution, long totalTimeMillis) {
        monitor.end(execution, totalTimeMillis);
    }

    protected void start(StageExecution execution) {
        monitor.start(execution);
    }
}
