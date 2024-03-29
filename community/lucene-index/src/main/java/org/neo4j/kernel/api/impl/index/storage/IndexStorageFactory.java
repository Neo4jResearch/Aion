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
package org.neo4j.kernel.api.impl.index.storage;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;

public class IndexStorageFactory implements AutoCloseable {
    private final DirectoryFactory dirFactory;
    private final FileSystemAbstraction fileSystem;
    private final IndexDirectoryStructure structure;

    public IndexStorageFactory(
            DirectoryFactory dirFactory, FileSystemAbstraction fileSystem, IndexDirectoryStructure structure) {
        this.dirFactory = dirFactory;
        this.fileSystem = fileSystem;
        this.structure = structure;
    }

    public PartitionedIndexStorage indexStorageOf(long indexId) {
        return new PartitionedIndexStorage(dirFactory, fileSystem, structure.directoryForIndex(indexId));
    }

    @Override
    public void close() throws Exception {
        dirFactory.close();
    }
}
