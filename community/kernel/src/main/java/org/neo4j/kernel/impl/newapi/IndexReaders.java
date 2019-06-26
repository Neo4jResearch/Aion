/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor2;
import org.neo4j.kernel.api.index.IndexReader;

import static org.neo4j.io.IOUtils.closeAllUnchecked;

class IndexReaders implements Closeable
{
    private final List<IndexReader> indexReaders = new ArrayList<>();
    private final IndexDescriptor2 descriptor;
    private final Read read;

    IndexReaders( IndexDescriptor2 descriptor, Read read )
    {
        this.descriptor = descriptor;
        this.read = read;
    }

    IndexReader createReader() throws IndexNotFoundKernelException
    {
        IndexReader indexReader = read.indexReader( descriptor, true );
        indexReaders.add( indexReader );
        return indexReader;
    }

    @Override
    public void close()
    {
        closeAllUnchecked( indexReaders );
    }
}
