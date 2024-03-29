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
package org.neo4j.server.http.cypher.format.jolt.v1;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.neo4j.server.http.cypher.format.jolt.AbstractJoltModule;

enum JoltModuleV1 {
    DEFAULT(new JoltModuleImpl(false)),
    STRICT(new JoltModuleImpl(true));

    private final SimpleModule instance;

    JoltModuleV1(SimpleModule instance) {
        this.instance = instance;
    }

    public SimpleModule getInstance() {
        return instance;
    }

    private static class JoltModuleImpl extends AbstractJoltModule {
        private JoltModuleImpl(boolean strictModeEnabled) {
            super(strictModeEnabled);

            this.addSerializer(new JoltNodeSerializer());
            this.addSerializer(new JoltRelationshipSerializer());
            this.addSerializer(new JoltRelationshipReversedSerializer());
            this.addSerializer(new JoltPathSerializer());
        }
    }
}
