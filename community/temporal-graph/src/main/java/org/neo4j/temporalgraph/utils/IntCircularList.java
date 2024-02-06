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

import java.util.Collection;
import java.util.NoSuchElementException;

public class IntCircularList {
    private int[] data;
    private int capacity;
    private int readIndex;
    private int writeIndex;
    private int elements;

    public IntCircularList() {
        capacity = 16;
        data = new int[capacity];
        readIndex = 0;
        writeIndex = capacity - 1;
        elements = 0;
    }

    public IntCircularList(Collection<Integer> other) {
        this.capacity = other.size();
        this.readIndex = 0;
        this.writeIndex = capacity - 1;
        this.elements = other.size();
        this.data = other.stream().mapToInt(Integer::intValue).toArray();
    }

    public IntCircularList(int[] array) {
        this.capacity = array.length;
        this.readIndex = 0;
        this.writeIndex = capacity - 1;
        this.elements = array.length;
        this.data = array;
    }

    public void add(int e) {
        if (elements == capacity) {
            capacity = 2 * capacity;
            var newData = new int[capacity];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
        }
        elements++;
        writeIndex++;
        if (writeIndex == capacity) {
            writeIndex = 0;
        }
        data[writeIndex] = e;
    }

    public int poll() {
        if (elements == 0) {
            throw new NoSuchElementException("The list is empty.");
        }
        elements--;
        if (readIndex == capacity) {
            readIndex = 0;
        }
        return data[readIndex++];
    }

    public int size() {
        return elements;
    }

    public void clear() {
        readIndex = 0;
        writeIndex = capacity - 1;
        elements = 0;
    }

    public boolean isEmpty() {
        return elements == 0;
    }
}
