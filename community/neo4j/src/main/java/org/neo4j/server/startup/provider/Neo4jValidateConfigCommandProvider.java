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
package org.neo4j.server.startup.provider;

import static org.neo4j.cli.CommandType.NEO4J_VALIDATE_CONFIG;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.CommandType;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.server.startup.ValidateConfigCommand;

@ServiceProvider
public class Neo4jValidateConfigCommandProvider implements CommandProvider {
    @Override
    public ValidateConfigCommand createCommand(ExecutionContext ctx) {
        return new ValidateConfigCommand(ctx);
    }

    @Override
    public CommandType commandType() {
        return NEO4J_VALIDATE_CONFIG;
    }
}
