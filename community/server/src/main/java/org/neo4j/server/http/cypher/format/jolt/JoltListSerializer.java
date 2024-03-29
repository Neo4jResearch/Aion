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
package org.neo4j.server.http.cypher.format.jolt;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.CollectionLikeType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.util.List;

final class JoltListSerializer extends StdSerializer<List<?>> {
    static final CollectionLikeType HANDLED_TYPE =
            TypeFactory.defaultInstance().constructCollectionLikeType(List.class, Object.class);

    JoltListSerializer() {
        super(HANDLED_TYPE);
    }

    @Override
    public void serialize(List<?> list, JsonGenerator generator, SerializerProvider provider) throws IOException {
        generator.writeStartObject(list);
        generator.writeFieldName(Sigil.LIST.getValue());
        generator.writeStartArray(list);

        for (var entry : list) {
            generator.writeObject(entry);
        }

        generator.writeEndArray();
        generator.writeEndObject();
    }
}
