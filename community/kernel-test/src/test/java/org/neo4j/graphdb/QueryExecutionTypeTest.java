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
package org.neo4j.graphdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.SCHEMA_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.WRITE;
import static org.neo4j.graphdb.QueryExecutionType.explained;
import static org.neo4j.graphdb.QueryExecutionType.profiled;
import static org.neo4j.graphdb.QueryExecutionType.query;

import java.lang.reflect.Field;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class QueryExecutionTypeTest {

    private static Stream<Arguments> parameters() {
        return Stream.of(
                of(verify(that(query(READ_ONLY)).canContainResults())),
                of(verify(that(query(READ_WRITE)).canContainResults().canUpdateData())),
                of(verify(that(query(WRITE)).canUpdateData())),
                of(verify(that(query(SCHEMA_WRITE)).canUpdateSchema())),
                // PROFILE
                of(verify(that(profiled(READ_ONLY)).isExplained().isProfiled().canContainResults())),
                of(verify(that(profiled(READ_WRITE))
                        .isExplained()
                        .isProfiled()
                        .canContainResults()
                        .canUpdateData())),
                of(verify(that(profiled(WRITE)).isExplained().isProfiled().canUpdateData())),
                of(verify(
                        that(profiled(SCHEMA_WRITE)).isExplained().isProfiled().canUpdateSchema())),
                // EXPLAIN
                of(verify(that(explained(READ_ONLY)).isExplained().isOnlyExplained())),
                of(verify(that(explained(READ_WRITE)).isExplained().isOnlyExplained())),
                of(verify(that(explained(WRITE)).isExplained().isOnlyExplained())),
                of(verify(that(explained(SCHEMA_WRITE)).isExplained().isOnlyExplained())),
                // query of EXPLAIN
                of(verify(thatQueryOf(explained(READ_ONLY)).canContainResults())),
                of(verify(thatQueryOf(explained(READ_WRITE)).canContainResults().canUpdateData())),
                of(verify(thatQueryOf(explained(WRITE)).canUpdateData())),
                of(verify(thatQueryOf(explained(SCHEMA_WRITE)).canUpdateSchema())));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void verifyTest(Assumptions expected) {
        QueryExecutionType executionType = expected.type();
        assertEquals(expected.isProfiled, executionType.isProfiled());
        assertEquals(expected.requestedExecutionPlanDescription, executionType.requestedExecutionPlanDescription());
        assertEquals(expected.isExplained, executionType.isExplained());
        assertEquals(expected.canContainResults, executionType.canContainResults());
        assertEquals(expected.canUpdateData, executionType.canUpdateData());
        assertEquals(expected.canUpdateSchema, executionType.canUpdateSchema());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void noneOtherLikeIt(Assumptions expected) {
        for (QueryExecutionType.QueryType queryType : QueryExecutionType.QueryType.values()) {
            for (QueryExecutionType type :
                    new QueryExecutionType[] {query(queryType), profiled(queryType), explained(queryType)}) {
                // the very same object will have the same flags, as will all the explained ones...
                if (type != expected.type() && !(expected.type().isExplained() && type.isExplained())) {
                    assertFalse(
                            expected.isProfiled == type.isProfiled()
                                    && expected.requestedExecutionPlanDescription
                                            == type.requestedExecutionPlanDescription()
                                    && expected.isExplained == type.isExplained()
                                    && expected.canContainResults == type.canContainResults()
                                    && expected.canUpdateData == type.canUpdateData()
                                    && expected.canUpdateSchema == type.canUpdateSchema(),
                            expected.type().toString());
                }
            }
        }
    }

    private static Object[] verify(Assumptions assumptions) {
        return new Object[] {assumptions};
    }

    private static Assumptions that(QueryExecutionType type) {
        return new Assumptions(type, false);
    }

    private static Assumptions thatQueryOf(QueryExecutionType type) {
        return new Assumptions(type, true);
    }

    static class Assumptions {
        final QueryExecutionType type;
        final boolean convertToQuery;
        boolean isProfiled;
        boolean requestedExecutionPlanDescription;
        boolean isExplained;
        boolean canContainResults;
        boolean canUpdateData;
        boolean canUpdateSchema;

        Assumptions(QueryExecutionType type, boolean convertToQuery) {
            this.type = type;
            this.convertToQuery = convertToQuery;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder(type.toString());
            if (convertToQuery) {
                result.append(" (as query)");
            }
            String sep = ": ";
            for (Field field : getClass().getDeclaredFields()) {
                if (field.getType() == boolean.class) {
                    boolean value;
                    field.setAccessible(true);
                    try {
                        value = field.getBoolean(this);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    result.append(sep)
                            .append('.')
                            .append(field.getName())
                            .append("() == ")
                            .append(value);
                    sep = ", ";
                }
            }
            return result.toString();
        }

        Assumptions isProfiled() {
            this.isProfiled = true;
            return this;
        }

        Assumptions isExplained() {
            this.requestedExecutionPlanDescription = true;
            return this;
        }

        Assumptions isOnlyExplained() {
            this.isExplained = true;
            return this;
        }

        Assumptions canContainResults() {
            this.canContainResults = true;
            return this;
        }

        Assumptions canUpdateData() {
            this.canUpdateData = true;
            return this;
        }

        Assumptions canUpdateSchema() {
            this.canUpdateSchema = true;
            return this;
        }

        public QueryExecutionType type() {
            return convertToQuery ? query(type.queryType()) : type;
        }
    }
}
