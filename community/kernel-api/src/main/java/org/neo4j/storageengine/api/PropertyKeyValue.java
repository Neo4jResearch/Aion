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
package org.neo4j.storageengine.api;

import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Default implementation of {@link StorageProperty} where the {@link Value} has already been materialized.
 */
public class PropertyKeyValue implements StorageProperty {
    private final int propertyKeyId;
    private final Value value;

    public PropertyKeyValue(int propertyKeyId, Value value) {
        assert value != null;
        this.propertyKeyId = propertyKeyId;
        this.value = value;
    }

    @Override
    public int propertyKeyId() {
        return propertyKeyId;
    }

    @Override
    public Value value() {
        return value;
    }

    @Override
    public boolean isDefined() {
        return value != Values.NO_VALUE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PropertyKeyValue that = (PropertyKeyValue) o;

        return propertyKeyId == that.propertyKeyId && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = propertyKeyId;
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Property{" + propertyKeyId + "=" + value + '}';
    }

    @Override
    public String toString(TokenHolder tokenHolder) {
        String propertyKeyName;
        try {
            propertyKeyName = "'" + tokenHolder.getTokenById(propertyKeyId).name() + "'";
        } catch (TokenNotFoundException e) {
            propertyKeyName = String.valueOf(propertyKeyId);
        }
        return "Property{" + propertyKeyName + "=" + value + '}';
    }
}
