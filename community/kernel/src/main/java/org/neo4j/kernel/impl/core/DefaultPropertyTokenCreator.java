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
package org.neo4j.kernel.impl.core;

import java.util.function.Supplier;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;

public class DefaultPropertyTokenCreator extends IsolatedTransactionTokenCreator {
    public DefaultPropertyTokenCreator(Supplier<Kernel> kernelSupplier) {
        super(kernelSupplier);
    }

    @Override
    protected int createKey(KernelTransaction transaction, String name, boolean internal) throws KernelException {
        return transaction.tokenWrite().propertyKeyCreateForName(name, internal);
    }
}
