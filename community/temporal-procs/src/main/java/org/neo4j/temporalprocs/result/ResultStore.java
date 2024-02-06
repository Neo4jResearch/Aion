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
package org.neo4j.temporalprocs.result;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResultStore {
    private final Map<String, Object> results;

    private ResultStore() {
        results = new ConcurrentHashMap<>();
    }

    private static class SingletonHelper {
        private static final ResultStore INSTANCE = new ResultStore();
    }

    public static ResultStore getInstance() {
        return ResultStore.SingletonHelper.INSTANCE;
    }

    public void put(String key, Object value) {
        results.put(key, value);
    }

    public Object get(String key) {
        return results.get(key);
    }

    public void clear() {
        results.clear();
    }
}
