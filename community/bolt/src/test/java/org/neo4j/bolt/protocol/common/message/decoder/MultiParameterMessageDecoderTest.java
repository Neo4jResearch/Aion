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
package org.neo4j.bolt.protocol.common.message.decoder;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public interface MultiParameterMessageDecoderTest<D extends MessageDecoder<?>> extends MessageDecoderTest<D> {

    default int insufficientNumberOfFields() {
        return this.minimumNumberOfFields() - 1;
    }

    default int minimumNumberOfFields() {
        return 2;
    }

    @Override
    default int maximumNumberOfFields() {
        return this.minimumNumberOfFields();
    }

    @Override
    default int excessNumberOfFields() {
        return this.maximumNumberOfFields() + 1;
    }

    @Test
    default void shouldFailWithIllegalStructSizeWhenInsufficientNumberOfFieldsIsGiven() {
        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> this.getDecoder()
                        .read(
                                ConnectionMockFactory.newInstance(),
                                PackstreamBuf.allocUnpooled(),
                                new StructHeader(this.insufficientNumberOfFields(), (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be " + this.minimumNumberOfFields()
                        + " fields but got " + this.insufficientNumberOfFields());
    }
}
