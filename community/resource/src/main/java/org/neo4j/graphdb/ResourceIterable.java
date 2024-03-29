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
package org.neo4j.graphdb;

import java.util.stream.Stream;
import org.neo4j.annotations.api.PublicApi;

/**
 * {@link Iterable} whose {@link ResourceIterator iterators} have associated resources
 * that need to be released.
 *
 * {@link ResourceIterator ResourceIterators} are always automatically released when their owning
 * transaction is committed or rolled back.
 *
 * Inside a long running transaction, it is possible to release associated resources early. To do so
 * you must ensure that all returned ResourceIterators are either fully exhausted, or explicitly closed.
 * <p>
 * If you intend to exhaust the returned iterators, and you are sure that the code within the iteration will <b>NOT</b> throw any exceptions,
 * you can use conventional code as you would with a normal Iterable:
 *
 * <pre>
 * {@code
 * ResourceIterable<Object> iterable;
 * for ( Object item : iterable )
 * {
 *     ...
 * }
 * }
 * </pre>
 *
 * However, if your code might not exhaust the iterator, (run until {@link java.util.Iterator#hasNext()}
 * returns {@code false}), {@link ResourceIterator} provides you with a {@link ResourceIterator#close()} method that
 * can be invoked to release its associated resources early, by using a {@code finally}-block, or try-with-resource.
 *
 * <pre>
 * {@code
 * ResourceIterable<Object> iterable;
 * ResourceIterator<Object> iterator = iterable.iterator();
 * try
 * {
 *     while ( iterator.hasNext() )
 *     {
 *         Object item = iterator.next();
 *         if ( ... )
 *         {
 *             return item; // iterator may not be exhausted.
 *         }
 *     }
 * }
 * finally
 * {
 *     iterator.close();
 * }
 * }
 * </pre>
 *
 * @param <T> the type of values returned through the iterators
 *
 * @see ResourceIterator
 */
@PublicApi
public interface ResourceIterable<T> extends Iterable<T>, Resource {
    /**
     * Returns an {@link ResourceIterator iterator} with associated resources that may be managed.
     */
    @Override
    ResourceIterator<T> iterator();

    /**
     * Returns an {@link Stream} of associated resources that may be managed. Care should be taken to
     * {@link Stream#close()} the stream to also close the underlying {@link ResourceIterable}.
     *
     * @return this iterable as a {@link Stream}
     */
    default Stream<T> stream() {
        return iterator().stream().onClose(this::close);
    }
}
