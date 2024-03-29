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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.exceptions.RuntimeUnsupportedException

object CommunityRuntimeFactory {

  val interpreted =
    new FallbackRuntime[RuntimeContext](List(SchemaCommandRuntime, InterpretedRuntime), CypherRuntimeOption.interpreted)

  val slotted = new FallbackRuntime[RuntimeContext](
    List(SchemaCommandRuntime, CommunitySlottedRuntime),
    CypherRuntimeOption.slotted
  )

  val default =
    new FallbackRuntime[RuntimeContext](
      List(SchemaCommandRuntime, CommunitySlottedRuntime),
      CypherRuntimeOption.default
    )

  def getRuntime(cypherRuntime: CypherRuntimeOption, disallowFallback: Boolean): CypherRuntime[RuntimeContext] =
    cypherRuntime match {
      case CypherRuntimeOption.legacy => interpreted

      case CypherRuntimeOption.interpreted => slotted

      case CypherRuntimeOption.slotted => slotted

      case CypherRuntimeOption.default => default

      case unsupported if disallowFallback =>
        throw new RuntimeUnsupportedException(s"This version of Neo4j does not support requested runtime: $unsupported")

      case unsupported => new FallbackRuntime[RuntimeContext](
          List(UnknownRuntime(unsupported.name), SchemaCommandRuntime, InterpretedRuntime),
          unsupported
        )
    }
}
