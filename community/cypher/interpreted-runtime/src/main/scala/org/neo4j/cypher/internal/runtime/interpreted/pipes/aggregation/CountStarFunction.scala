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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

class CountStarFunction extends AggregationFunction {
  var count: Long = 0

  override def apply(data: ReadableRow, state: QueryState): Unit = {
    count += 1
  }

  override def result(state: QueryState): AnyValue = Values.longValue(count)
}

object CountStarFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[CountStarFunction])
}
