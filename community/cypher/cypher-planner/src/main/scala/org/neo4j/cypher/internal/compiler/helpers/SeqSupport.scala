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
package org.neo4j.cypher.internal.compiler.helpers

import scala.util.control.TailCalls
import scala.util.control.TailCalls.TailRec

object SeqSupport {

  implicit class RichSeq[T](inner: Seq[T]) {

    /** A mix of map and foldLeft.
     *
     * Applies the function `f` to each element of the sequence going from left to right, threading an accumulator through each call,
     * returning both the accumulated value and the mapped sequence.
     *
     * Example:
     * {{{
     *   scala> val result = Seq("FOO", "BAR", "BAZ").foldMap(1) {
     *     case (index, string) => (index + 1, s"$index.$string")
     *   }
     *   result: (Int, List[String]) = (4,List(1.FOO, 2.BAR, 3.BAZ))
     * }}}
     *
     * @param acc the initial value of the accumulator
     * @param f binary function from the current accumulator and an element of the sequence to both the new accumulator and the mapped element
     * @tparam A the type of the accumulator
     * @return a tuple of: the final value of the accumulator, and the mapped sequence
     */
    def foldMap[A](acc: A)(f: (A, T) => (A, T)): (A, Seq[T]) = {
      val builder = Seq.newBuilder[T]
      var current = acc

      for (element <- inner) {
        val (newAcc, newElement) = f(current, element)
        current = newAcc
        builder += newElement
      }

      (current, builder.result())
    }

    def asNonEmptyOption: Option[Seq[T]] = if (inner.isEmpty) None else Some(inner)

    /** Partitions this sequence, grouping values into a sequences according to some discriminator function,
     * preserving the order in which the values are first encountered.
     *
     * For example:
     * {{{
     *   scala> val groups = Seq("foo", "", "bar").sequentiallyGroupBy(_.length)
     *   groups: Seq[(Int, Seq[String])] = List((3,List(foo, bar)), (0,List("")))
     *   }}}
     *
     * Compare with the default `groupBy` implementation, note how the resulting sequence starts with the second value, there is no guaranteed order:
     * {{{
     *   scala> val groups = Seq("foo", "", "bar").groupBy(_.length).toSeq
     *   groups: Seq[(Int, Seq[String])] = List((0,List("")), (3,List(foo, bar)))
     * }}}
     *
     * @param f the discriminator function.
     * @tparam K the type of keys returned by the discriminator function.
     * @return A sequence of tuples each containing a key `k = f(x)` and all the elements `x` of the sequence where `f(x)` is equal to `k`.
     */
    def sequentiallyGroupBy[K](f: T => K): Seq[(K, Seq[T])] = IterableHelper.sequentiallyGroupBy(inner)(f)

    /**
     * Analogous to `Seq.forall` but for `TailRec[Boolean]`.
     */
    def forallTailRec(f: T => TailRec[Boolean]): TailRec[Boolean] = {
      if (inner.isEmpty) TailCalls.done(true)
      else {
        TailCalls.tailcall(f(inner.head)).flatMap {
          case false => TailCalls.done(false)
          case true  => inner.tail.forallTailRec(f)
        }
      }
    }
  }
}
