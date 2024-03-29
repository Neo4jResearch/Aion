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
package org.neo4j.packstream.error.reader;

import org.neo4j.kernel.api.exceptions.Status;

public class LimitExceededException extends PackstreamReaderException implements Status.HasStatus {
    private final long limit;
    private final long actual;

    public LimitExceededException(long limit, long actual) {
        super("Value of size " + actual + " exceeded limit of " + limit);

        this.limit = limit;
        this.actual = actual;
    }

    public long getLimit() {
        return this.limit;
    }

    public long getActual() {
        return this.actual;
    }

    @Override
    public Status status() {
        return Status.Request.Invalid;
    }
}
