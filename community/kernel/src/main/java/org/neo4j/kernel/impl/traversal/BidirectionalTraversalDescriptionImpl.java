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
package org.neo4j.kernel.impl.traversal;

import static org.neo4j.graphdb.traversal.BranchCollisionPolicies.STANDARD;
import static org.neo4j.graphdb.traversal.SideSelectorPolicies.ALTERNATING;
import static org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription.addEvaluator;
import static org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription.nullCheck;

import java.util.Arrays;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.BranchCollisionPolicy;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.SideSelectorPolicy;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

public class BidirectionalTraversalDescriptionImpl implements BidirectionalTraversalDescription {
    final MonoDirectionalTraversalDescription start;
    final MonoDirectionalTraversalDescription end;
    final PathEvaluator collisionEvaluator;
    final SideSelectorPolicy sideSelector;
    final org.neo4j.graphdb.traversal.BranchCollisionPolicy collisionPolicy;
    final int maxDepth;

    private BidirectionalTraversalDescriptionImpl(
            MonoDirectionalTraversalDescription start,
            MonoDirectionalTraversalDescription end,
            org.neo4j.graphdb.traversal.BranchCollisionPolicy collisionPolicy,
            PathEvaluator collisionEvaluator,
            SideSelectorPolicy sideSelector,
            int maxDepth) {
        this.start = start;
        this.end = end;
        this.collisionPolicy = collisionPolicy;
        this.collisionEvaluator = collisionEvaluator;
        this.sideSelector = sideSelector;
        this.maxDepth = maxDepth;
    }

    public BidirectionalTraversalDescriptionImpl() {
        this(
                new MonoDirectionalTraversalDescription(),
                new MonoDirectionalTraversalDescription(),
                STANDARD,
                Evaluators.all(),
                ALTERNATING,
                Integer.MAX_VALUE);
    }

    @Override
    public BidirectionalTraversalDescription startSide(TraversalDescription startSideDescription) {
        assertIsMonoDirectional(startSideDescription);
        return new BidirectionalTraversalDescriptionImpl(
                (MonoDirectionalTraversalDescription) startSideDescription,
                this.end,
                this.collisionPolicy,
                this.collisionEvaluator,
                this.sideSelector,
                this.maxDepth);
    }

    @Override
    public BidirectionalTraversalDescription endSide(TraversalDescription endSideDescription) {
        assertIsMonoDirectional(endSideDescription);
        return new BidirectionalTraversalDescriptionImpl(
                this.start,
                (MonoDirectionalTraversalDescription) endSideDescription,
                this.collisionPolicy,
                this.collisionEvaluator,
                this.sideSelector,
                this.maxDepth);
    }

    @Override
    public BidirectionalTraversalDescription mirroredSides(TraversalDescription sideDescription) {
        assertIsMonoDirectional(sideDescription);
        return new BidirectionalTraversalDescriptionImpl(
                (MonoDirectionalTraversalDescription) sideDescription,
                (MonoDirectionalTraversalDescription) sideDescription.reverse(),
                collisionPolicy,
                collisionEvaluator,
                sideSelector,
                maxDepth);
    }

    @Override
    public BidirectionalTraversalDescription collisionPolicy(BranchCollisionPolicy collisionPolicy) {
        return new BidirectionalTraversalDescriptionImpl(
                this.start, this.end, collisionPolicy, this.collisionEvaluator, this.sideSelector, this.maxDepth);
    }

    @Override
    public BidirectionalTraversalDescription collisionEvaluator(PathEvaluator collisionEvaluator) {
        nullCheck(collisionEvaluator, Evaluator.class, "RETURN_ALL");
        return new BidirectionalTraversalDescriptionImpl(
                this.start,
                this.end,
                this.collisionPolicy,
                addEvaluator(this.collisionEvaluator, collisionEvaluator),
                this.sideSelector,
                maxDepth);
    }

    @Override
    public BidirectionalTraversalDescription collisionEvaluator(Evaluator collisionEvaluator) {
        return collisionEvaluator(new Evaluator.AsPathEvaluator(collisionEvaluator));
    }

    @Override
    public BidirectionalTraversalDescription sideSelector(SideSelectorPolicy sideSelector, int maxDepth) {
        return new BidirectionalTraversalDescriptionImpl(
                this.start, this.end, this.collisionPolicy, this.collisionEvaluator, sideSelector, maxDepth);
    }

    @Override
    public Traverser traverse(Node start, Node end) {
        return traverse(Arrays.asList(start), Arrays.asList(end));
    }

    @Override
    public Traverser traverse(final Iterable<Node> startNodes, final Iterable<Node> endNodes) {
        return new DefaultTraverser(() -> new BidirectionalTraverserIterator(
                start, end, sideSelector, collisionPolicy, collisionEvaluator, maxDepth, startNodes, endNodes));
    }

    /**
     * We currently only support mono-directional traversers as "inner" traversers, so we need to check specifically
     * for this when the user specifies traversers to work with.
     */
    private static void assertIsMonoDirectional(TraversalDescription traversal) {
        if (!(traversal instanceof MonoDirectionalTraversalDescription)) {
            throw new IllegalArgumentException("The bi-directional traversals currently do not support using "
                    + "anything but mono-directional traversers as start and stop points. Please provide a regular "
                    + "mono-directional traverser instead.");
        }
    }
}
