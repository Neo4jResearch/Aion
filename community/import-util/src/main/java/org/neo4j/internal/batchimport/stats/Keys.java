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
package org.neo4j.internal.batchimport.stats;

/**
 * Common {@link Stat statistic} keys.
 */
public enum Keys implements Key {
    received_batches(">", "Number of batches received from upstream"),
    done_batches("!", "Number of batches processed and done, and sent off downstream"),
    total_processing_time("=", "Total processing time for all done batches"),
    /**
     * The difference from {@link #total_processing_time} is that it's the total processing time divided by number of processors,
     * giving more a wall clock indication how long this step has taken.
     */
    total_processing_wall_clock_time(
            "≡",
            "Total wall clock processing time for all done batches, i.e. total processing time divided by processors"),
    upstream_idle_time("^", "Time spent waiting for batch from upstream"),
    downstream_idle_time("v", "Time spent waiting for downstream to catch up"),
    avg_processing_time("avg", "Average processing time per done batch"),
    io_throughput(null, "I/O throughput per second"),
    memory_usage(null, "Memory usage"),
    progress(null, "Progress"); // overrides progress calculation using done_batches, if this stat exists

    private final String shortName;
    private final String description;

    Keys(String shortName, String description) {
        this.shortName = shortName;
        this.description = description;
    }

    @Override
    public String shortName() {
        return shortName;
    }

    @Override
    public String description() {
        return description;
    }
}
