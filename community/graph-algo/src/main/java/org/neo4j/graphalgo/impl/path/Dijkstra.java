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
package org.neo4j.graphalgo.impl.path;

import static org.neo4j.graphalgo.impl.util.PathInterestFactory.single;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.internal.helpers.collection.Iterators.firstOrNull;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.util.DijkstraSelectorFactory;
import org.neo4j.graphalgo.impl.util.PathInterest;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;
import org.neo4j.graphalgo.impl.util.WeightedPathIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

/**
 * Find (one or some) simple shortest path(s) between two nodes.
 * Shortest referring to least cost evaluated by provided {@link CostEvaluator}.
 *
 * When asking for {@link #findAllPaths(Node, Node)} behaviour will depending on
 * which {@link PathInterest} is used.
 * Recommended option is
 * {@link PathInterestFactory#numberOfShortest(double,int)} - defined number of shortest path in increasing order
 *
 * Also available
 * {@link PathInterestFactory#allShortest(double)}          - Find all paths that are equal in length to shortest.
 *                                                            {@link DijkstraBidirectional} does this faster.
 * {@link PathInterestFactory#all(double)}                  - Find all paths in increasing order. This option has
 *                                                            performance problem and is not recommended.
 */
public class Dijkstra implements PathFinder<WeightedPath> {
    private final PathExpander<Double> expander;
    private final InitialBranchState<Double> stateFactory;
    private final CostEvaluator<Double> costEvaluator;
    private Traverser lastTraverser;
    private final double epsilon;
    private final PathInterest<Double> interest;
    // TODO: ALso set traverser to always use DijkstraPathExpander and DijkstraEvaluator.

    /**
     * Construct new dijkstra algorithm.
     * @param expander          {@link PathExpander} to be used to decide which relationships
     *                          to expand.
     * @param costEvaluator     {@link CostEvaluator} to be used to calculate cost of relationship
     * @param epsilon           The tolerance level to be used when comparing floating point numbers.
     * @param interest          {@link PathInterest} to be used when deciding if a path is interesting.
     *                          Recommend to use {@link PathInterestFactory} to get reliable behaviour.
     */
    public Dijkstra(
            PathExpander<Double> expander,
            CostEvaluator<Double> costEvaluator,
            double epsilon,
            PathInterest<Double> interest) {
        this.expander = expander;
        this.costEvaluator = costEvaluator;
        this.epsilon = epsilon;
        this.interest = interest;
        this.stateFactory = InitialBranchState.DOUBLE_ZERO;
    }

    @Override
    public Iterable<WeightedPath> findAllPaths(Node start, final Node end) {
        final Traverser traverser = traverser(start, end, interest);
        return () -> new WeightedPathIterator(traverser.iterator(), costEvaluator, epsilon, interest);
    }

    private Traverser traverser(Node start, final Node end, PathInterest<Double> interest) {
        MutableDouble shortestSoFar = new MutableDouble(Double.MAX_VALUE);
        PathExpander<Double> dijkstraExpander =
                new DijkstraPathExpander(expander, shortestSoFar, epsilon, interest.stopAfterLowestCost());
        PathEvaluator<Double> dijkstraEvaluator = new DijkstraEvaluator(shortestSoFar, end, costEvaluator);

        lastTraverser = new MonoDirectionalTraversalDescription()
                .uniqueness(Uniqueness.NODE_PATH)
                .expand(dijkstraExpander, stateFactory)
                .order(new DijkstraSelectorFactory(interest, costEvaluator))
                .evaluator(dijkstraEvaluator)
                .traverse(start);
        return lastTraverser;
    }

    @Override
    public WeightedPath findSinglePath(Node start, Node end) {
        return firstOrNull(new WeightedPathIterator(
                traverser(start, end, single(epsilon)).iterator(), costEvaluator, epsilon, interest));
    }

    @Override
    public TraversalMetadata metadata() {
        return lastTraverser.metadata();
    }

    private static class DijkstraPathExpander implements PathExpander<Double> {
        protected final PathExpander<Double> source;
        protected MutableDouble shortestSoFar;
        private final double epsilon;
        protected final boolean stopAfterLowestCost;

        DijkstraPathExpander(
                final PathExpander<Double> source,
                MutableDouble shortestSoFar,
                double epsilon,
                boolean stopAfterLowestCost) {
            this.source = source;
            this.shortestSoFar = shortestSoFar;
            this.epsilon = epsilon;
            this.stopAfterLowestCost = stopAfterLowestCost;
        }

        @Override
        public ResourceIterable<Relationship> expand(Path path, BranchState<Double> state) {
            if (MathUtil.compare(state.getState(), shortestSoFar.doubleValue(), epsilon) > 0 && stopAfterLowestCost) {
                return Iterables.emptyResourceIterable();
            }
            return source.expand(path, state);
        }

        @Override
        public PathExpander<Double> reverse() {
            return new DijkstraPathExpander(source.reverse(), shortestSoFar, epsilon, stopAfterLowestCost);
        }
    }

    private static class DijkstraEvaluator extends PathEvaluator.Adapter<Double> {
        private final MutableDouble shortestSoFar;
        private final Node endNode;
        private final CostEvaluator<Double> costEvaluator;

        DijkstraEvaluator(MutableDouble shortestSoFar, Node endNode, CostEvaluator<Double> costEvaluator) {
            this.shortestSoFar = shortestSoFar;
            this.endNode = endNode;
            this.costEvaluator = costEvaluator;
        }

        @Override
        public Evaluation evaluate(Path path, BranchState<Double> state) {
            double nextState = state.getState();
            if (path.length() > 0) {
                nextState += costEvaluator.getCost(path.lastRelationship(), OUTGOING);
                state.setState(nextState);
            }
            if (path.endNode().equals(endNode)) {
                shortestSoFar.setValue(Math.min(shortestSoFar.doubleValue(), nextState));
                return Evaluation.INCLUDE_AND_PRUNE;
            }
            return Evaluation.EXCLUDE_AND_CONTINUE;
        }
    }
}
