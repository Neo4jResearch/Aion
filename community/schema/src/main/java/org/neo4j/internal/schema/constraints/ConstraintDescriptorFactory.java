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
package org.neo4j.internal.schema.constraints;

import java.util.List;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaValueType;

public class ConstraintDescriptorFactory {
    private ConstraintDescriptorFactory() {}

    public static NodeExistenceConstraintDescriptor existsForLabel(int labelId, int... propertyIds) {
        return ConstraintDescriptorImplementation.makeExistsConstraint(
                SchemaDescriptors.forLabel(labelId, propertyIds));
    }

    public static RelExistenceConstraintDescriptor existsForRelType(int relTypeId, int... propertyIds) {
        return ConstraintDescriptorImplementation.makeExistsConstraint(
                SchemaDescriptors.forRelType(relTypeId, propertyIds));
    }

    public static UniquenessConstraintDescriptor uniqueForLabel(int labelId, int... propertyIds) {
        return uniqueForSchema(SchemaDescriptors.forLabel(labelId, propertyIds));
    }

    public static UniquenessConstraintDescriptor uniqueForLabel(IndexType indexType, int labelId, int... propertyIds) {
        return uniqueForSchema(SchemaDescriptors.forLabel(labelId, propertyIds), indexType);
    }

    public static KeyConstraintDescriptor nodeKeyForLabel(int labelId, int... propertyIds) {
        return keyForSchema(SchemaDescriptors.forLabel(labelId, propertyIds));
    }

    public static KeyConstraintDescriptor nodeKeyForLabel(IndexType indexType, int labelId, int... propertyIds) {
        return keyForSchema(SchemaDescriptors.forLabel(labelId, propertyIds), indexType);
    }

    public static ConstraintDescriptor existsForSchema(SchemaDescriptor schema) {
        ConstraintDescriptorImplementation constraint = ConstraintDescriptorImplementation.makeExistsConstraint(schema);
        if (schema.isLabelSchemaDescriptor()) {
            return constraint.asNodePropertyExistenceConstraint();
        }
        if (schema.isRelationshipTypeSchemaDescriptor()) {
            return constraint.asRelationshipPropertyExistenceConstraint();
        }
        throw new UnsupportedOperationException("Cannot create existence constraint for the given schema.");
    }

    public static NodeExistenceConstraintDescriptor existsForSchema(LabelSchemaDescriptor schema) {
        return ConstraintDescriptorImplementation.makeExistsConstraint(schema);
    }

    public static RelExistenceConstraintDescriptor existsForSchema(RelationTypeSchemaDescriptor schema) {
        return ConstraintDescriptorImplementation.makeExistsConstraint(schema);
    }

    public static UniquenessConstraintDescriptor uniqueForSchema(SchemaDescriptor schema) {
        return ConstraintDescriptorImplementation.makeUniqueConstraint(schema, IndexType.RANGE);
    }

    public static UniquenessConstraintDescriptor uniqueForSchema(SchemaDescriptor schema, IndexType indexType) {
        return ConstraintDescriptorImplementation.makeUniqueConstraint(schema, indexType);
    }

    public static KeyConstraintDescriptor keyForSchema(SchemaDescriptor schema) {
        return ConstraintDescriptorImplementation.makeUniqueExistsConstraint(schema, IndexType.RANGE);
    }

    public static KeyConstraintDescriptor keyForSchema(SchemaDescriptor schema, IndexType indexType) {
        return ConstraintDescriptorImplementation.makeUniqueExistsConstraint(schema, indexType);
    }

    public static TypeConstraintDescriptor typeForSchema(SchemaDescriptor schema, List<SchemaValueType> allowedTypes) {
        return ConstraintDescriptorImplementation.makePropertyTypeConstraint(schema, allowedTypes);
    }
}
