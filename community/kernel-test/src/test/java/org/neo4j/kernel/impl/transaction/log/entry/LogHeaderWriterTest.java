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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.V8;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.decodeLogFormatVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.decodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.LOG_VERSION_MASK;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdSerialization;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class LogHeaderWriterTest {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private RandomSupport random;

    private long expectedLogVersion;
    private long expectedTxId;
    private StoreId expectedStoreId;
    private int expectedBlockSize;
    private int expectedChecksum;

    @BeforeEach
    void setUp() {
        expectedLogVersion = random.nextLong(0, LOG_VERSION_MASK);
        expectedTxId = random.nextLong(0, Long.MAX_VALUE);
        expectedStoreId = new StoreId(
                random.nextLong(),
                random.nextLong(),
                "engine-" + random.nextInt(0, 255),
                "format-" + random.nextInt(0, 255),
                random.nextInt(0, 127),
                random.nextInt(0, 127));
        expectedBlockSize = 1 << random.nextInt(7, 10);
        expectedChecksum = random.nextInt();
    }

    @ParameterizedTest
    @EnumSource(
            mode = EXCLUDE,
            names = {"V6", "V7"}) // We don't support writing v6 and v7
    void shouldWriteALogHeaderInAStoreChannel(LogFormat logFormat) throws IOException {
        // given
        final var file = testDirectory.file("WriteLogHeader");
        final var channel = fileSystem.write(file);
        LogHeader logHeader = new LogHeader(
                logFormat.getVersionByte(),
                new LogPosition(expectedLogVersion, logFormat.getHeaderSize()),
                expectedTxId,
                expectedStoreId,
                expectedBlockSize,
                expectedChecksum);

        // when
        writeLogHeader(channel, logHeader, INSTANCE);

        channel.close();

        // then
        final var array = new byte[logFormat.getHeaderSize()];
        try (var stream = fileSystem.openAsInputStream(file)) {
            assertEquals(logFormat.getHeaderSize(), stream.read(array));
        }
        final var result = ByteBuffer.wrap(array);

        final var encodedLogVersions = result.getLong();
        final var txId = result.getLong();
        StoreId storeId = StoreIdSerialization.deserializeWithFixedSize(result);

        assertEquals(encodeLogVersion(expectedLogVersion, logFormat.getVersionByte()), encodedLogVersions);
        assertEquals(logFormat.getVersionByte(), decodeLogFormatVersion(encodedLogVersions));
        assertEquals(expectedLogVersion, decodeLogVersion(encodedLogVersions));
        assertEquals(expectedTxId, txId);
        assertEquals(expectedStoreId, storeId);

        if (logFormat != V8) {
            assertEquals(expectedBlockSize, result.getInt());
            assertEquals(expectedChecksum, result.getInt());
        }
    }
}
