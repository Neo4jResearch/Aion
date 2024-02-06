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
package org.neo4j.fabric.parser;

import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;

public record ProcedureStruct(
        String dbName,
        int type,
        long entityId,
        int hops,
        int direction,
        long systemStartTime,
        long systemEndTime,
        long applicationStartTime,
        long applicationEndTime) {
    @Override
    public String toString() {
        return String.format(
                "DB %s, type %d, entityId %d, hops %d, direction %d, system time [%d, %d], application time [%d, %d]",
                dbName,
                type,
                entityId,
                hops,
                direction,
                systemStartTime,
                systemEndTime,
                applicationStartTime,
                applicationEndTime);
    }

    public static RunMessage convertToMessage(ProcedureStruct struct, RunMessage prev) {
        if (struct.systemStartTime != struct.systemEndTime && struct.type != 1) {
            throw new UnsupportedOperationException(String.format(
                    "Time range queries are not supported yet: %d != %d",
                    struct.systemStartTime, struct.systemEndTime));
        }

        String statement;
        switch (struct.type) {
            case 0 -> statement = String.format(
                    "CALL lineage.getTemporalRelationship(%d, %d);", struct.entityId(), struct.systemStartTime);
            case 1 -> statement = (struct.systemStartTime == struct.systemEndTime)
                    ? String.format("CALL lineage.getTemporalNode(%d, %d);", struct.entityId(), struct.systemStartTime)
                    : String.format(
                            "CALL lineage.getTemporalNodeHistory(%d, %d, %d);",
                            struct.entityId(), struct.systemStartTime, struct.systemEndTime);
            case 2 -> statement = (struct.hops() <= 2)
                    ? String.format(
                            "CALL lineage.expandNode(%d, %d, %d, %d);",
                            struct.entityId(), struct.direction(), struct.hops(), struct.systemStartTime)
                    : String.format(
                            "CALL time.expandNode(%d, %d, %d, %d);",
                            struct.entityId(), struct.direction(), struct.hops(), struct.systemStartTime);
            case 3 -> statement = String.format("CALL time.getTemporalAllRelationships(%d);", struct.systemStartTime);
            case 4 -> statement = (struct.applicationStartTime != -1 && struct.applicationEndTime != -1)
                    ? String.format(
                            "CALL time.getTemporalAllNodesWithAT(%d, %d, %d);",
                            struct.systemStartTime, struct.applicationStartTime, struct.applicationEndTime)
                    : String.format("CALL time.getTemporalAllNodes(%d);", struct.systemStartTime);
            default -> throw new RuntimeException(String.format("Unknown procedure type %d", struct.type));
        }

        return createMessage(prev, statement);
    }

    private static RunMessage createMessage(RunMessage prev, String statement) {
        return new RunMessage(
                statement,
                prev.params(),
                prev.bookmarks(),
                prev.transactionTimeout(),
                prev.getAccessMode(),
                prev.transactionMetadata(),
                prev.databaseName(),
                prev.impersonatedUser(),
                prev.notificationsConfig());
    }
}
