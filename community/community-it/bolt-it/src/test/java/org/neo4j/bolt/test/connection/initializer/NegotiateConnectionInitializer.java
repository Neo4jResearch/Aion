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
package org.neo4j.bolt.test.connection.initializer;

import java.io.IOException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.neo4j.bolt.test.annotation.connection.initializer.Negotiated;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;

/**
 * Performs the Bolt protocol negotiation upon connection creation.
 * <p />
 * This initializer may be registered via the {@link Negotiated} annotation or any of its children.
 */
public final class NegotiateConnectionInitializer implements ConnectionInitializer {

    @Override
    public void initialize(
            ExtensionContext extensionContext, ParameterContext context, BoltWire wire, TransportConnection connection)
            throws ParameterResolutionException {
        try {
            wire.negotiate(connection);
        } catch (IOException ex) {
            throw new ParameterResolutionException(
                    "Failed to negotiate protocol revision " + wire.getProtocolVersion(), ex);
        }
    }
}
