/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import builders._
import org.neo4j.cypher.internal.compiler.v2_1._
import commands._
import pipes._
import profiler.Profiler
import org.neo4j.cypher.PeriodicCommitInOpenTransactionException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.compiler.v2_1.planner.{CantHandleQueryException, Planner}
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_1.spi.{UpdateCountingQueryContext, CSVResources, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_1.commands.PeriodicCommitQuery
import org.neo4j.cypher.internal.compiler.v2_1.commands.Union
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_1.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_1.helpers.{EagerMappingBuilder, MappingBuilder}

case class PipeInfo(pipe: Pipe, updating: Boolean, periodicCommit: Option[PeriodicCommitInfo] = None)

case class PeriodicCommitInfo(size: Option[Long]) {
  def provideBatchSize = size.getOrElse(/* defaultSize */ 10000L)
}

class ExecutionPlanBuilder(graph: GraphDatabaseService, pipeBuilder: PipeBuilder = new PipeBuilder, execPlanBuilder: Planner = new Planner()) extends PatternGraphBuilder {

  def build(planContext: PlanContext, inputQuery: AbstractQuery, ast: Statement): ExecutionPlan = {

    val PipeInfo(p, isUpdating, periodicCommitInfo) = try {
      execPlanBuilder.producePlan(ast)
    } catch {
      case _: CantHandleQueryException => pipeBuilder.buildPipes(planContext, inputQuery)
    }

    val columns = getQueryResultColumns(inputQuery, p.symbols)
    val func = getExecutionPlanFunction(p, columns, periodicCommitInfo, isUpdating)

    new ExecutionPlan {
      def execute(queryContext: QueryContext, params: Map[String, Any]) = func(queryContext, params, false)

      def profile(queryContext: QueryContext, params: Map[String, Any]) = func(new UpdateCountingQueryContext(queryContext), params, true)
    }
  }

  private def getQueryResultColumns(q: AbstractQuery, currentSymbols: SymbolTable): List[String] = q match {
    case in: PeriodicCommitQuery =>
      getQueryResultColumns(in.query, currentSymbols)

    case in: Query =>
      // Find the last query part
      var query = in
      while (query.tail.isDefined) {
        query = query.tail.get
      }

      query.returns.columns.flatMap {
        case "*" => currentSymbols.identifiers.keys
        case x => Seq(x)
      }

    case union: Union =>
      getQueryResultColumns(union.queries.head, currentSymbols)

    case _ =>
      List.empty
  }

  private def getExecutionPlanFunction(pipe: Pipe,
                                       columns: List[String],
                                       periodicCommit: Option[PeriodicCommitInfo],
                                       updating: Boolean) =
    (queryContext: QueryContext, params: Map[String, Any], profile: Boolean) => {

      val builder = new ExecutionWorkflowBuilder(queryContext)

      if (periodicCommit.isDefined) {
        if (!queryContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()

        val batchSize = periodicCommit.get.provideBatchSize
        if (!pipe.exists(_.isInstanceOf[LoadCSVPipe]))
          builder.setPeriodicCommitObserver(batchSize)
        else
          builder.setLoadCsvPeriodicCommitObserver(batchSize)
      }

      builder.transformQueryContext(new UpdateCountingQueryContext(_))

      if (profile)
        builder.setDecorator(new Profiler())

      builder.runWithQueryState(graph, params) {
        state =>
          val results = pipe.createResults(state)
          val closingIterator = builder.buildClosingIterator(results)
          val descriptor = builder.buildDescriptor(pipe, closingIterator.isEmpty)

          if (updating)
            new EagerPipeExecutionResult(closingIterator, columns, state, descriptor)
          else
            new PipeExecutionResult(closingIterator, columns, state, descriptor)
      }
    }
}

class ExecutionWorkflowBuilder(initialQueryContext: QueryContext) {
  private val taskCloser = new TaskCloser
  private val externalResource: ExternalResource = new CSVResources(taskCloser)
  private val queryContextBuilder: MappingBuilder[QueryContext] = new EagerMappingBuilder[QueryContext](initialQueryContext)
  private var decorator: PipeDecorator = NullDecorator

  def transformQueryContext(f: QueryContext => QueryContext) {
    queryContextBuilder += f
  }

  def setPeriodicCommitObserver(batchSize: Long) {
    addUpdateObserver(new PeriodicCommitObserver(batchSize, queryContext))
  }

  def setLoadCsvPeriodicCommitObserver(batchSize: Long) {
    addUpdateObserver(new LoadCsvPeriodicCommitObserver(batchSize, externalResource, queryContext))
  }

  def setDecorator(newDecorator: PipeDecorator) {
    decorator = newDecorator
  }

  def buildClosingIterator(results: Iterator[ExecutionContext]) = new ClosingIterator(results, taskCloser)

  def buildDescriptor(pipe: Pipe, isProfileReady: => Boolean) =
    () => decorator.decorate(pipe.executionPlanDescription, isProfileReady)

  def runWithQueryState[T](graph: GraphDatabaseService, params: Map[String, Any])(f: QueryState => T) = {
    taskCloser.addTask(queryContext.close)
    val state = new QueryState(graph, queryContext, externalResource, params, decorator)
    try {
      f(state)
    }
    catch {
      case (t: Throwable) =>
        taskCloser.close(success = false)
        throw t
    }
  }

  private def queryContext = queryContextBuilder.result()

  private def addUpdateObserver(newObserver: UpdateObserver) {
    transformQueryContext(new UpdateObservableQueryContext(newObserver, _))
  }
}
