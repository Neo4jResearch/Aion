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
package org.neo4j.bolt.tx;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import org.junit.jupiter.api.Timeout;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

/**
 * Ensures that Bolt terminates transactions when {@code RESET} is received.
 */
@TestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class TransactionTerminationIT {

    @Inject
    private Neo4jWithSocket server;

    private void awaitTransactionStart() throws InterruptedException {
        long txCount = 1;
        while (txCount <= 1) {
            try (var tx = server.graphDatabaseService().beginTx()) {
                var result = tx.execute("SHOW TRANSACTIONS");
                txCount = result.stream().toList().size();
            }

            Thread.sleep(100);
        }
    }

    @Timeout(15)
    @ProtocolTest
    void killTxViaReset(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        connection.send(wire.begin()).send(wire.run("UNWIND range(1, 2000000) AS i CREATE (n)"));

        awaitTransactionStart();

        connection.send(wire.reset());

        assertThat(connection)
                .receivesSuccess()
                .receivesFailure(Status.Transaction.Terminated, Status.Transaction.LockClientStopped)
                .receivesSuccess();
    }
}
