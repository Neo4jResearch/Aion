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
package org.neo4j.counts;

import org.neo4j.io.pagecache.context.CursorContext;

public interface CountsVisitor {
    @FunctionalInterface
    interface Visitable {
        void accept(CountsVisitor visitor, CursorContext cursorContext);
    }

    void visitNodeCount(int labelId, long count);

    void visitRelationshipCount(int startLabelId, int typeId, int endLabelId, long count);

    class Adapter implements CountsVisitor {
        @Override
        public void visitNodeCount(int labelId, long count) {
            // override in subclasses
        }

        @Override
        public void visitRelationshipCount(int startLabelId, int typeId, int endLabelId, long count) {
            // override in subclasses
        }
    }
}
