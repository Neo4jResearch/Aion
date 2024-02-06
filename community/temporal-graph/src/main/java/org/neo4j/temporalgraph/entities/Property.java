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
package org.neo4j.temporalgraph.entities;

import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public record Property(PropertyType type, String name, Object value) {
    public static Value wrapAsValue(Object value) {
        if (value instanceof Long || value instanceof LongValue) {
            return Values.longValue((long) value);
        } else if (value instanceof Integer || value instanceof IntegralValue) {
            return Values.intValue((int) value);
        } else if (value instanceof Double || value instanceof DoubleValue) {
            return Values.doubleValue((double) value);
        } else {
            throw new UnsupportedOperationException(String.format("Cannot recognize the type of %s", value.toString()));
        }
    }

    public int getSize() {
        return switch (type) {
            case LONG -> Long.BYTES;
            case INT -> Integer.BYTES;
            case FLOAT -> Float.BYTES;
            case DOUBLE -> Double.BYTES;
            default -> throw new UnsupportedOperationException(String.format("Type %s is not supported", type));
        };
    }
}
