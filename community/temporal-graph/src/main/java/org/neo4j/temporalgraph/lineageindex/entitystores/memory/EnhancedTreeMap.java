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
package org.neo4j.temporalgraph.lineageindex.entitystores.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.temporalgraph.entities.InMemoryEntity;
import org.neo4j.temporalgraph.entities.InMemoryEntityFactory;

public class EnhancedTreeMap<K, V extends InMemoryEntity> {

    TreeMap<K, List<Pair<Long, V>>> tree;

    Comparator<Pair<Long, V>> comparator = Comparator.comparing(Pair::getLeft);

    public EnhancedTreeMap(Comparator<K> keyComp) {
        tree = new TreeMap<>(keyComp);
    }

    public void put(K key, V value) {
        tree.putIfAbsent(key, new ArrayList<>());
        var entries = tree.get(key);

        // todo: create full entries if a threshold is reached
        /*if (!entries.isEmpty() && !value.isDeleted()) {
            value.setDiff();
        }*/
        insertInPlace(entries, new ImmutablePair<>(value.getStartTimestamp(), value));
    }

    public Optional<V> get(K key, long timestamp) {
        var entries = tree.get(key);
        return constructEntityFromEntries(entries, timestamp);
    }

    public Optional<V> getFirstEntry(K key) {
        var entries = tree.get(key);
        if (entries == null) {
            return Optional.empty();
        }

        return Optional.of(constructEntity(entries, 0));
    }

    public Optional<V> getLastEntry(K key) {
        var entries = tree.get(key);
        if (entries == null) {
            return Optional.empty();
        }

        return Optional.of(constructEntity(entries, entries.size() - 1));
    }

    public boolean setDeleted(K key, long timestamp) {
        var entries = tree.get(key);
        if (entries == null || entries.get(entries.size() - 1).getRight().getStartTimestamp() != timestamp) {
            return false;
        }
        entries.get(entries.size() - 1).getRight().setDeleted();
        return true;
    }

    public List<V> getAll(long timestamp) {
        var result = new ArrayList<V>();
        for (var key : tree.keySet()) {
            var element = get(key, timestamp);
            element.ifPresent(result::add);
        }
        return result;
    }

    public List<V> rangeScanByKey(K fromKey, K toKey, long timestamp) {
        var result = new ArrayList<V>();
        var subMap = tree.subMap(fromKey, toKey);
        for (var e : subMap.entrySet()) {
            var element = constructEntityFromEntries(e.getValue(), timestamp);
            element.ifPresent(result::add);
        }
        return result;
    }

    public List<V> rangeScanByTime(K key, long fromTimestamp, long toTimestamp) {
        var entries = tree.get(key);

        var result = new ArrayList<V>();
        var currentPos = Math.max(searchForTime(entries, fromTimestamp), 0);
        var currentNode =
                Optional.of((V) InMemoryEntityFactory.newEntity(entries.get(0).getRight()));
        for (; currentPos < entries.size(); currentPos++) {
            var entry = entries.get(currentPos);
            if (entry.getRight().getStartTimestamp() > toTimestamp) {
                break;
            }

            currentNode.get().merge(entry.getRight());
            updateLastElement(result, currentNode.get().getStartTimestamp());
            if (!currentNode.get().isDeleted()) {
                result.add(currentNode.get());
                currentNode = Optional.of((V) currentNode.get().copy());
            } else {
                currentNode = Optional.of(
                        (V) InMemoryEntityFactory.newEntity(entries.get(0).getRight()));
            }
        }
        return result;
    }

    public List<List<V>> rangeScanByKeyAndTime(K fromKey, K toKey, long fromTimestamp, long toTimestamp) {
        var result = new ArrayList<List<V>>();
        var subMap = tree.subMap(fromKey, toKey);
        for (var e : subMap.entrySet()) {
            var subResult = new ArrayList<V>();
            for (var update : e.getValue()) {
                var updateTimestamp = update.getRight().getStartTimestamp();
                if (updateTimestamp >= fromTimestamp && updateTimestamp <= toTimestamp) {
                    // todo: optimize the result construction by reusing previous results
                    var element = constructEntityFromEntries(e.getValue(), updateTimestamp);
                    element.ifPresent(subResult::add);
                }
            }
            result.add(subResult);
        }
        return result;
    }

    private void insertInPlace(List<Pair<Long, V>> values, Pair<Long, V> value) {
        int pos = Collections.binarySearch(values, value, comparator);
        if (pos < 0) {
            values.add(-pos - 1, value);
        } else {
            values.add(pos + 1, value);
        }
    }

    private Optional<V> constructEntityFromEntries(List<Pair<Long, V>> entries, long timestamp) {
        if (entries == null) {
            return Optional.empty();
        }

        var pos = searchForTime(entries, timestamp);

        if (pos < 0 || entries.get(pos).getRight().isDeleted()) {
            return Optional.empty();
        }
        // Reconstruct entity
        return Optional.of(constructEntity(entries, pos));
    }

    private int searchForTime(List<Pair<Long, V>> entries, long timestamp) {
        // search for the first entry that has a timestamp larger or equal than the argument provided
        int pos = Collections.binarySearch(entries, new ImmutablePair<>(timestamp, null), comparator);
        pos = (pos < 0) ? -pos - 2 : pos;
        return pos;
    }

    private V constructEntity(List<Pair<Long, V>> entries, int pos) {
        // If this is the first element of the chain, or this is a complete record (i.e., not a diff),
        // or the previous entity was deleted we found the result
        if (pos == 0
                || !entries.get(pos).getRight().isDiff()
                || entries.get(pos - 1).getRight().isDeleted()) {
            return entries.get(pos).getRight();
        }

        // Otherwise, we need to reconstruct it. First, we need to find from which point to start.
        int firstPos = pos;
        while (firstPos >= 0) {
            if (entries.get(pos).getRight().isDeleted()) {
                break;
            }

            firstPos--;

            if (firstPos == 0
                    || (!entries.get(pos).getRight().isDiff()
                            || entries.get(pos).getRight().isDeleted())) {
                break;
            }
        }

        // Construct final result
        var result = InMemoryEntityFactory.newEntity(entries.get(0).getRight());
        for (int i = firstPos; i <= pos; ++i) {
            result.merge(entries.get(i).getRight());
        }

        return (V) result;
    }

    public void reset() {
        tree.clear();
    }

    private void updateLastElement(List<V> result, long timestamp) {
        if (!result.isEmpty()) {
            result.get(result.size() - 1).setEndTimestamp(timestamp);
        }
    }
}
