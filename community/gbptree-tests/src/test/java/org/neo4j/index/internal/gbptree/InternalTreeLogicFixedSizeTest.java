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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;

import org.apache.commons.lang3.mutable.MutableLong;

class InternalTreeLogicFixedSizeTest extends InternalTreeLogicTestBase<MutableLong, MutableLong> {
    private final SimpleLongLayout layout = longLayout().build();

    @Override
    protected ValueMerger<MutableLong, MutableLong> getAdder() {
        return (existingKey, newKey, base, add) -> {
            base.add(add.longValue());
            return ValueMerger.MergeResult.MERGED;
        };
    }

    @Override
    protected TreeNode<MutableLong, MutableLong> getTreeNode(
            int pageSize,
            Layout<MutableLong, MutableLong> layout,
            OffloadStore<MutableLong, MutableLong> offloadStore) {
        return new TreeNodeFixedSize<>(pageSize, layout);
    }

    @Override
    protected TestLayout<MutableLong, MutableLong> getLayout() {
        return layout;
    }
}
