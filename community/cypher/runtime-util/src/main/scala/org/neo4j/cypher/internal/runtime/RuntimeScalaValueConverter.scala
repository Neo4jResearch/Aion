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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.util.Eagerly.immutableMapValues

import java.lang
import java.util

import scala.collection.immutable
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

// Converts java runtime values to scala runtime values
//
// Main use: Converting results when using ExecutionEngine from scala
//
class RuntimeScalaValueConverter(skip: Any => Boolean) {

  final def asDeepScalaMap[A, B](map: util.Map[A, B]): immutable.Map[A, Any] =
    if (map == null) null else immutableMapValues(map.asScala, asDeepScalaValue): immutable.Map[A, Any]

  final def asShallowScalaMap[A, B](map: util.Map[A, B]): immutable.Map[A, Any] =
    if (map == null) null else map.asScala.toMap

  def asDeepScalaValue(value: Any): Any = value match {
    case anything if skip(anything)        => anything
    case javaMap: util.Map[_, _]           => immutableMapValues(javaMap.asScala, asDeepScalaValue)
    case javaList: java.util.LinkedList[_] => copyJavaList(javaList, () => new util.LinkedList[Any]())
    case javaList: java.util.List[_]       => copyJavaList(javaList, () => new util.ArrayList[Any](javaList.size()))
    case javaIterable: lang.Iterable[_]    => javaIterable.asScala.map(asDeepScalaValue).toIndexedSeq: IndexedSeq[_]
    case map: collection.Map[_, _]         => immutableMapValues(map, asDeepScalaValue): immutable.Map[_, _]
    case iterableOnce: IterableOnce[_]     => iterableOnce.iterator.map(asDeepScalaValue).toIndexedSeq: IndexedSeq[_]
    case anything                          => anything
  }

  def asShallowScalaValue(value: Any): Any = value match {
    case anything if skip(anything)     => anything
    case javaMap: util.Map[_, _]        => javaMap.asScala.toMap: immutable.Map[_, _]
    case javaIterable: lang.Iterable[_] => javaIterable.asScala.toIndexedSeq: IndexedSeq[_]
    case map: collection.Map[_, _]      => map.toMap: immutable.Map[_, _]
    case anything                       => anything
  }

  private def copyJavaList(list: java.util.List[_], newList: () => java.util.List[Any]) = {
    val copy = newList()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      copy.add(iterator.next())
    }
    JavaListWrapper(copy, this)
  }
}
