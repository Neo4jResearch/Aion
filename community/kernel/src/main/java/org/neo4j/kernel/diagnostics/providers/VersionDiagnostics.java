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
package org.neo4j.kernel.diagnostics.providers;

import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.NamedDiagnosticsProvider;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.Version;
import org.neo4j.storageengine.api.StoreId;

public class VersionDiagnostics extends NamedDiagnosticsProvider {
    private final DbmsInfo dbmsInfo;
    private final StoreId storeId;

    VersionDiagnostics(DbmsInfo dbmsInfo, StoreId storeId) {
        super("Version");
        this.dbmsInfo = dbmsInfo;
        this.storeId = storeId;
    }

    @Override
    public void dump(DiagnosticsLogger logger) {
        logger.log("DBMS: " + dbmsInfo + " " + storeId.getStoreVersionUserString());
        logger.log("Kernel version: " + Version.getKernelVersion());
    }
}
