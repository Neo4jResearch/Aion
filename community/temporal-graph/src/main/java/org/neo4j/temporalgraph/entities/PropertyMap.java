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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class PropertyMap {
    private Property[] properties;
    private int propertiesSize;

    public PropertyMap() {
        propertiesSize = 0;
        properties = new Property[propertiesSize];
    }

    public void put(Property property) {
        // Search if it already exists
        for (int i = 0; i < propertiesSize; ++i) {
            if (properties[i].name().equals(property.name())) {
                properties[i] = property;
                return;
            }
        }
        // Otherwise add it in the end
        resizeProperties();
        properties[propertiesSize] = property;
        propertiesSize++;
    }

    public void putAll(PropertyMap otherList) {
        for (int i = 0; i < otherList.size(); ++i) {
            var p = otherList.properties[i];
            put(p);
        }
    }

    public void removeDeleted() {
        for (int i = propertiesSize - 1; i >= 0; --i) {
            if (properties[i].value() == null) {
                if (i + 1 < propertiesSize) {
                    for (int j = i + 1; j < propertiesSize; j++) {
                        properties[j - 1] = properties[j];
                    }
                }
                propertiesSize--;
            }
        }
    }

    public void clear() {
        propertiesSize = 0;
        properties = new Property[propertiesSize];
    }

    public List<Property> getPropertiesAsList() {
        return new ArrayList<>(Arrays.asList(properties).subList(0, propertiesSize));
    }

    public Property[] getProperties() {
        return properties;
    }

    public int size() {
        return propertiesSize;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(properties) ^ Objects.hashCode(propertiesSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final PropertyMap other = (PropertyMap) obj;
        if (other.size() != this.size()) {
            return false;
        }

        for (int i = 0; i < this.size(); ++i) {
            if (!exists(other.properties[i])) {
                return false;
            }
        }
        return true;
    }

    private boolean exists(Property property) {
        for (Property value : properties) {
            if (value.name().equals(property.name())) {
                return value.equals(property);
            }
        }
        return false;
    }

    private void resizeProperties() {
        if (propertiesSize + 1 >= properties.length) {
            var newSize = Math.max(2 * propertiesSize, 1);
            var newP = new Property[newSize];
            System.arraycopy(properties, 0, newP, 0, properties.length);

            properties = newP;
        }
    }
}
