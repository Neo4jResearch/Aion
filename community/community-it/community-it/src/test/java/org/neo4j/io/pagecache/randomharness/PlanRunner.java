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
package org.neo4j.io.pagecache.randomharness;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

class PlanRunner implements Callable<Void> {
    private final Plan plan;
    private final AtomicBoolean stopSignal;

    PlanRunner(Plan plan, AtomicBoolean stopSignal) {
        this.plan = plan;
        this.stopSignal = stopSignal;
    }

    @Override
    public Void call() throws Exception {
        Action action = plan.next();
        while (action != null && !stopSignal.get()) {
            try {
                action.perform();
            } catch (Exception ignore) {
            }
            action = plan.next();
        }
        return null;
    }
}
