/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition

case class ListLiteral(expressions: Seq[Expression])(val position: InputPosition) extends Expression {

  def map(f: Expression => Expression) = copy(expressions = expressions.map(f))(position)

  override def asCanonicalStringVal: String = expressions.map(_.asCanonicalStringVal).mkString("[", ", ", "]")

  override def isConstantForQuery: Boolean = expressions.forall(_.isConstantForQuery)
}

case class ListSlice(list: Expression, from: Option[Expression], to: Option[Expression])(val position: InputPosition)
    extends Expression {
  override def isConstantForQuery: Boolean = list.isConstantForQuery && from.forall(_.isConstantForQuery)
}

case class ContainerIndex(expr: Expression, idx: Expression)(val position: InputPosition)
    extends Expression {
  override def isConstantForQuery: Boolean = expr.isConstantForQuery && idx.isConstantForQuery
}
