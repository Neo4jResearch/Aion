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
package org.neo4j.internal.kernel.api.exceptions.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

public class IndexNotFoundKernelException extends KernelException {
    private final IndexDescriptor index;

    public IndexNotFoundKernelException(String msg) {
        super(Status.Schema.IndexNotFound, msg);
        this.index = null;
    }

    public IndexNotFoundKernelException(String msg, IndexDescriptor index) {
        super(Status.Schema.IndexNotFound, msg);
        this.index = index;
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        if (index == null) {
            return super.getUserMessage(tokenNameLookup);
        } else {
            return super.getUserMessage(tokenNameLookup) + index.userDescription(tokenNameLookup);
        }
    }
}
