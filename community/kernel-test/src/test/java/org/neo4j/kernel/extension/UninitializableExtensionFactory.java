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
package org.neo4j.kernel.extension;

import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * This kernel extension cannot be initialised, because an exception will be thrown we the machinery tries to create a proxy of the UnproxyableDepencies class.
 */
public class UninitializableExtensionFactory extends ExtensionFactory<UnproxyableDependencies> {
    public UninitializableExtensionFactory() {
        super("uninitializable");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, UnproxyableDependencies dependencies) {
        return null;
    }
}
