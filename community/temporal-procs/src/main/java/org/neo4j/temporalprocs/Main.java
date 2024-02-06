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

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Scanner;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.temporalgraph.lineageindex.EntityLineageTracker;
import org.neo4j.temporalgraph.timeindex.SnapshotCreationPolicy;
import org.neo4j.temporalgraph.timeindex.timestore.TimeBasedTracker;

public class Main {

    private static final Path DB_PATH = Path.of("target/neo4j-store-with-time-tracking");
    private static EntityLineageTracker lineageTracker = null;
    private static TimeBasedTracker timeBasedTracker = null;

    public static void main(String[] args) throws IOException {

        var embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder(DB_PATH)
                .withProcedure(LineageStoreProcedures.class)
                .withProcedure(TimeStoreProcedures.class)
                .withConfig(BoltConnector.enabled, true)
                .withConfig(BoltConnector.listen_address, new SocketAddress("localhost", 7687))
                .build();

        registerTracker(
                embeddedDatabaseServer.databaseManagementService(),
                embeddedDatabaseServer.config().get(GraphDatabaseSettings.neo4j_home),
                0);
        registerTracker(
                embeddedDatabaseServer.databaseManagementService(),
                embeddedDatabaseServer.config().get(GraphDatabaseSettings.neo4j_home),
                1);

        var input = new Scanner(System.in);
        System.out.println("Press 0 to close the server:");
        while (true) {
            if (input.nextInt() == 0) {
                System.out.println("Closing the server...");
                break;
            }
        }

        closeTrackers();
        embeddedDatabaseServer.close();
    }

    private static void registerTracker(DatabaseManagementService dbms, Path dbPath, int type) throws IOException {
        var pageCache = (PageCache) dbms.database(DEFAULT_DATABASE_NAME).getPageCache();
        var fs = (FileSystemAbstraction) dbms.database(DEFAULT_DATABASE_NAME).getFileSystem();
        if (type == 0) {
            var nodeIndexPath = dbPath.toAbsolutePath().resolve("data/NODE_STORE_INDEX");
            var relIndexPath = dbPath.toAbsolutePath().resolve("data/REL_STORE_INDEX");
            lineageTracker = new EntityLineageTracker(pageCache, fs, nodeIndexPath, relIndexPath);
            dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, lineageTracker);
        } else if (type == 1) {
            var policy = new SnapshotCreationPolicy(10_000);
            var nodeIndexPath = dbPath.toAbsolutePath().resolve("data/DATA_LOG");
            var relIndexPath = dbPath.toAbsolutePath().resolve("data/TIME_INDEX");
            timeBasedTracker = new TimeBasedTracker(policy, pageCache, fs, nodeIndexPath, relIndexPath);
            dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, timeBasedTracker);
        } else {
            throw new IllegalArgumentException(String.format("Type %d is not supported", type));
        }
    }

    private static void closeTrackers() throws IOException {
        if (lineageTracker != null) {
            lineageTracker.shutdown();
        }
        if (timeBasedTracker != null) {
            timeBasedTracker.shutdown();
        }
    }
}
