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
package org.neo4j.temporalgraph.utils;

import static java.nio.file.Files.notExists;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.stream.Stream;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class DBUtils {
    private DBUtils() {}

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss");

    public static int nodeCapacity = 16;
    public static int relCapacity = 16;

    public static DatabaseManagementService getEmbeddedDatabaseManagementService(Path path) {
        /*return new DatabaseManagementServiceBuilder(path)
        // .setConfig(GraphDatabaseSettings.)
        .setConfig(GraphDatabaseSettings.auth_enabled, true)
        .setConfig(OnlineBackupSettings.online_backup_enabled, true)
        .setConfig(BoltConnector.enabled, true)
        .setConfig(BoltConnector.listen_address, new SocketAddress("localhost", 7687))
        .build();*/
        return null;
    }

    public static void registerShutdownHook(final DatabaseManagementService managementService) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread(managementService::shutdown));
    }

    public static void flush(GraphDatabaseService db) throws IOException {
        var forceOperation = ((GraphDatabaseAPI) db)
                .getDependencyResolver()
                .resolveDependency(CheckPointerImpl.ForceOperation.class);
        forceOperation.flushAndForce(DatabaseFlushEvent.NULL, CursorContext.NULL_CONTEXT);
    }

    public static void createNewDirectory(Path path) throws IOException {
        if (notExists(path)) {
            Files.createDirectories(path);
        }
    }

    public static String getSnapshotFileNameFromDirectory(Path dir, int position) throws IOException {
        try (Stream<Path> stream = Files.list(dir)) {
            var files = new java.util.ArrayList<>(stream.filter(file -> !Files.isDirectory(file))
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .toList());
            Collections.sort(files);

            if (files.size() <= position) {
                throw new IllegalStateException("Invalid number of files: " + files.size());
            }
            return files.get(position);
        }
    }

    public static FileSystemAbstraction getFileSystem(Database database) {
        return database.getDependencyResolver().resolveDependency(FileSystemAbstraction.class);
    }
}
