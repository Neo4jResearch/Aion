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
package org.neo4j.temporalprocs;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.gds.transaction.EmptyTransactionContext;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

public class ProcBase {
    @Context
    public Log log;

    @Context
    public GraphDatabaseService gds;

    @Context
    public Transaction procedureTransaction;

    @Context
    public KernelTransaction transaction;

    @Context
    public TaskRegistryFactory taskRegistryFactory;

    @Context
    public UserLogRegistryFactory userLogRegistryFactory;

    @Context
    public Username username = Username.EMPTY_USERNAME;

    protected final void validateGraphName(String username, String graphName) {
        if (GraphStoreCatalog.exists(username, databaseId(), graphName)) {
            throw new IllegalArgumentException("Unimplemented functionality");
        }
    }

    protected DatabaseId databaseId() {
        return DatabaseId.of(gds);
    }

    protected String username() {
        return username.username();
    }

    protected GraphLoaderContext graphLoaderContext() {
        return ImmutableGraphLoaderContext.builder()
                .databaseId(databaseId())
                .dependencyResolver(GraphDatabaseApiProxy.dependencyResolver(gds))
                .transactionContext(transactionContext())
                .log(log)
                .taskRegistryFactory(taskRegistryFactory)
                .userLogRegistryFactory(userLogRegistryFactory)
                // .terminationFlag(TerminationFlag.wrap(new TransactionTerminationMonitor(transaction)))
                .build();
    }

    public TransactionContext transactionContext() {
        return gds == null
                ? EmptyTransactionContext.INSTANCE
                : DatabaseTransactionContext.of(gds, procedureTransaction);
    }
}
