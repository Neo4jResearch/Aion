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
package org.neo4j.logging;

import java.util.function.Predicate;
import org.assertj.core.api.Assertions;

public final class LogAssertions extends Assertions {
    private LogAssertions() {}

    public static LogAssert assertThat(AssertableLogProvider logProvider) {
        return new LogAssert(logProvider);
    }

    public static Predicate<String> greaterThan(int threshold) {
        return new Predicate<>() {
            @Override
            public boolean test(String value) {
                return Integer.parseInt(value) > threshold;
            }

            @Override
            public String toString() {
                return "int > " + threshold;
            }
        };
    }
}
