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
package org.neo4j.internal.schema;

/**
 * A SchemaProcessor performs from side-effect processing on a SchemaDescriptor. To get the concrete type of the
 * target schema descriptor, a visitor pattern is used to bounce the code path into the correct overloaded
 * processSpecific variant. See {@link SchemaDescriptor#processWith(SchemaProcessor)}.
 */
public interface SchemaProcessor {
    /*
    The following section contains the overloaded process signatures for all concrete SchemaDescriptor implementers.
    Add new overloaded methods here when adding more concrete SchemaDescriptors.
     */
    void processSpecific(LabelSchemaDescriptor schema);

    void processSpecific(RelationTypeSchemaDescriptor schema);

    void processSpecific(SchemaDescriptor schema);
}
