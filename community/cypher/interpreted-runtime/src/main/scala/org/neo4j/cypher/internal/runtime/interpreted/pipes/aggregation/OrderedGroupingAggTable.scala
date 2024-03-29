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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedAggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedChunkReceiver
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue

/**
 * Specialization of [[GroupingAggTable]] where we have grouping columns with provided order and grouping columns without provided order.
 *
 * This table will only use the unordered grouping columns as a key in hash map. The ordered grouping columns are used to determine
 * when to use a new HashMap and discard the old one.
 *
 * @param orderedGroupingFunction a precomputed function to calculate the grouping key part of the ordered grouping columns
 * @param orderedGroupingColumns all grouping columns that have a provided order
 * @param unorderedGroupingFunction a precomputed function to calculate the grouping key part of the unordered grouping columns
 * @param unorderedGroupingColumns all grouping columns that do not have a provided order
 * @param aggregations all aggregation columns
 */
class OrderedGroupingAggTable(
  orderedGroupingFunction: (CypherRow, QueryState) => AnyValue,
  orderedGroupingColumns: Array[DistinctPipe.GroupingCol],
  unorderedGroupingFunction: (CypherRow, QueryState) => AnyValue,
  unorderedGroupingColumns: Array[DistinctPipe.GroupingCol],
  aggregations: Array[AggregationPipe.AggregatingCol],
  state: QueryState,
  rowFactory: CypherRowFactory,
  operatorId: Id
) extends GroupingAggTable(
      unorderedGroupingColumns,
      unorderedGroupingFunction,
      aggregations,
      state,
      rowFactory,
      operatorId
    ) with OrderedChunkReceiver {

  private var currentGroupKey: AnyValue = _

  override def close(): Unit = {
    currentGroupKey = null
    super.close()
  }

  override def clear(): Unit = {
    currentGroupKey = null
    super.clear()
  }

  override def isSameChunk(first: CypherRow, current: CypherRow): Boolean = {
    if (currentGroupKey == null) {
      currentGroupKey = orderedGroupingFunction(first, state)
    }
    current.eq(first) || currentGroupKey == orderedGroupingFunction(current, state)
  }

  override def result(): ClosingIterator[CypherRow] = {
    val addOrderedKeys = AggregationPipe.computeAddKeysToResultRowFunction(orderedGroupingColumns)
    super.result().map { row =>
      addOrderedKeys(row, currentGroupKey)
      row
    }
  }

  override def processNextChunk: Boolean = true
}

object OrderedGroupingAggTable {

  case class Factory(
    orderedGroupingFunction: (CypherRow, QueryState) => AnyValue,
    orderedGroupingColumns: Array[DistinctPipe.GroupingCol],
    unorderedGroupingFunction: (CypherRow, QueryState) => AnyValue,
    unorderedGroupingColumns: Array[DistinctPipe.GroupingCol],
    aggregations: Array[AggregationPipe.AggregatingCol]
  ) extends OrderedAggregationTableFactory {

    override def table(
      state: QueryState,
      rowFactory: CypherRowFactory,
      operatorId: Id
    ): AggregationTable with OrderedChunkReceiver =
      new OrderedGroupingAggTable(
        orderedGroupingFunction,
        orderedGroupingColumns,
        unorderedGroupingFunction,
        unorderedGroupingColumns,
        aggregations,
        state,
        rowFactory,
        operatorId
      )
  }
}
