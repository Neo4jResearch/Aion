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
package org.neo4j.cli;

public class CommandFailedException extends RuntimeException {
    private static final int DEFAULT_ERROR_EXIT_CODE = ExitCode.FAIL;

    private final int exitCode;

    public CommandFailedException(String message) {
        this(message, DEFAULT_ERROR_EXIT_CODE);
    }

    public CommandFailedException(String message, int exitCode) {
        super(message);
        this.exitCode = exitCode;
    }

    public CommandFailedException(String message, Throwable cause) {
        this(message, cause, DEFAULT_ERROR_EXIT_CODE);
    }

    public CommandFailedException(String message, Throwable cause, int exitCode) {
        super(message, cause);
        this.exitCode = exitCode;
    }

    public int getExitCode() {
        return exitCode;
    }
}
