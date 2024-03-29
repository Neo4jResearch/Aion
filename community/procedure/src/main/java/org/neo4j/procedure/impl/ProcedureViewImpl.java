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

package org.neo4j.procedure.impl;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.neo4j.collection.RawIterator;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.values.AnyValue;

public class ProcedureViewImpl implements ProcedureView {

    private final ProcedureRegistry registry;
    private final ComponentRegistry safeComponents;
    private final ComponentRegistry allComponents;

    public ProcedureViewImpl(
            ProcedureRegistry registryView, ComponentRegistry safeComponents, ComponentRegistry allComponents) {
        this.registry = registryView;
        this.safeComponents = safeComponents;
        this.allComponents = allComponents;
    }

    /**
     * Lookup registered component providers functions that capable to provide user requested type in scope of procedure invocation context
     * @param cls the type of registered component
     * @param safe set to false if desired component can bypass security, true if it respects security
     * @return registered provider function if registered, null otherwise
     */
    @Override
    public <T> ThrowingFunction<Context, T, ProcedureException> lookupComponentProvider(Class<T> cls, boolean safe) {
        var registryView = safe ? safeComponents : allComponents;
        return registryView.providerFor(cls);
    }

    @Override
    public ProcedureHandle procedure(QualifiedName name) throws ProcedureException {
        return registry.procedure(name);
    }

    @Override
    public UserFunctionHandle function(QualifiedName name) {
        return registry.function(name);
    }

    @Override
    public UserFunctionHandle aggregationFunction(QualifiedName name) {
        return registry.aggregationFunction(name);
    }

    @Override
    public int[] getIdsOfFunctionsMatching(Predicate<CallableUserFunction> predicate) {
        return registry.getIdsOfFunctionsMatching(predicate);
    }

    @Override
    public int[] getIdsOfAggregatingFunctionsMatching(Predicate<CallableUserAggregationFunction> predicate) {
        return registry.getIdsOfAggregatingFunctionsMatching(predicate);
    }

    @Override
    public Set<ProcedureSignature> getAllProcedures() {
        return registry.getAllProcedures();
    }

    @Override
    public int[] getIdsOfProceduresMatching(Predicate<CallableProcedure> predicate) {
        return registry.getIdsOfProceduresMatching(predicate);
    }

    @Override
    public Stream<UserFunctionSignature> getAllNonAggregatingFunctions() {
        return registry.getAllNonAggregatingFunctions();
    }

    @Override
    public Stream<UserFunctionSignature> getAllAggregatingFunctions() {
        return registry.getAllAggregatingFunctions();
    }

    @Override
    public RawIterator<AnyValue[], ProcedureException> callProcedure(
            Context ctx, int id, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {
        return registry.callProcedure(ctx, id, input, resourceMonitor);
    }

    @Override
    public AnyValue callFunction(Context ctx, int id, AnyValue[] input) throws ProcedureException {
        return registry.callFunction(ctx, id, input);
    }

    @Override
    public UserAggregationReducer createAggregationFunction(Context ctx, int id) throws ProcedureException {
        return registry.createAggregationFunction(ctx, id);
    }
}
