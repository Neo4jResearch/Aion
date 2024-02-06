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
package org.neo4j.temporalgraph.timeindex;

public class SnapshotCreationPolicy {
    public static final SnapshotCreationPolicy DEFAULT_POLICY = new SnapshotCreationPolicy(10_000);
    private final long numberOfChanges;
    private final long durationInMilliseconds;

    public SnapshotCreationPolicy(long numberOfChanges) {
        this(numberOfChanges, 86_400_000);
    }

    public SnapshotCreationPolicy(long numberOfChanges, long durationInMilliseconds) {
        this.numberOfChanges = numberOfChanges;
        this.durationInMilliseconds = durationInMilliseconds;
    }

    public boolean readyToTakeSnapshot(long currentChanges, long currentTime, long previousTime) {
        return readyToTakeSnapshot(currentChanges) || ((currentTime - previousTime) >= durationInMilliseconds);
    }

    public boolean readyToTakeSnapshot(long currentChanges) {
        return storeSnapshots() && (currentChanges >= numberOfChanges);
    }

    public boolean storeSnapshots() {
        return numberOfChanges != 0;
    }
}
