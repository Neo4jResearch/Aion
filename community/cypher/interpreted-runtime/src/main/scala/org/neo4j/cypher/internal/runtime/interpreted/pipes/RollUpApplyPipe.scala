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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.virtual.VirtualValues

case class RollUpApplyPipe(lhs: Pipe, rhs: Pipe, collectionName: String, identifierToCollect: String)(val id: Id =
  Id.INVALID_ID)
    extends PipeWithSource(lhs) {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.map {
      row =>
        val original = row.createClone()
        val innerState = state.withInitialContext(row)
        val innerResults = rhs.createResults(innerState)
        val collection = VirtualValues.list(innerResults.map(m => m.getByName(identifierToCollect)).toArray: _*)
        original.set(collectionName, collection)
        original
    }
  }
}
