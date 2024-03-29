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
package org.neo4j.bolt.protocol.common.message.encoder;

import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructWriter;

public final class SuccessMessageEncoder implements StructWriter<Connection, SuccessMessage> {
    private static final SuccessMessageEncoder INSTANCE = new SuccessMessageEncoder();

    private SuccessMessageEncoder() {}

    public static SuccessMessageEncoder getInstance() {
        return INSTANCE;
    }

    @Override
    public Class<SuccessMessage> getType() {
        return SuccessMessage.class;
    }

    @Override
    public short getTag(SuccessMessage payload) {
        return SuccessMessage.SIGNATURE;
    }

    @Override
    public long getLength(SuccessMessage payload) {
        return 1;
    }

    @Override
    public void write(Connection ctx, PackstreamBuf buffer, SuccessMessage payload) {
        ctx.writerContext(buffer).writeValue(payload.meta());
    }
}
