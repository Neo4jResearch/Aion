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
package org.neo4j.temporalgraph.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectArray<T> {
    private T[] data;
    private final AtomicInteger count;
    private int capacity;

    public ObjectArray(int capacity) {
        this.capacity = nextPowerOfTwo(capacity);
        count = new AtomicInteger();
        data = (T[]) new Object[this.capacity];
    }

    public void put(int index, T value) {
        resize(index);
        if (data[index] == null) {
            count.incrementAndGet();
        }
        data[index] = value;
    }

    public void remove(int index) {
        if (index >= capacity) {
            throw new IndexOutOfBoundsException(String.format("Index %d greater than capacity %d", index, capacity));
        }
        if (data[index] != null) {
            count.decrementAndGet();
        }
        data[index] = null;
    }

    public T get(int index) throws IndexOutOfBoundsException {
        if (index >= capacity) {
            return null;
        }

        return data[index];
    }

    public Object[] values() {
        return data;
    }

    public ObjectArrayIterator iterator() {
        return new ObjectArrayIterator(this);
    }

    public List<T> valuesAsList() {
        List<T> result = new ArrayList<>();
        for (var d : data) {
            if (d != null) {
                result.add(d);
            }
        }
        return result;
    }

    public int size() {
        return count.get();
    }

    public int maxSize() {
        return capacity;
    }

    public boolean contains(int index) {
        return index < capacity && data[index] != null;
    }

    public void clear() {
        for (int i = 0; i < capacity; ++i) {
            data[i] = null;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(data) ^ Objects.hashCode(capacity);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final var other = (ObjectArray<T>) obj;
        if (other.capacity != this.capacity || other.count.get() != this.count.get()) {
            return false;
        }

        for (int i = 0; i < this.capacity; ++i) {
            if ((this.data[i] == null && other.data[i] != null)
                    || (this.data[i] != null && !this.data[i].equals(other.data[i]))) {
                return false;
            }
        }
        return true;
    }

    private void resize(int index) {
        if (index >= capacity) {
            var newCapacity = nextPowerOfTwo(index);
            var newD = (T[]) new Object[newCapacity];
            System.arraycopy(data, 0, newD, 0, capacity);
            data = newD;
            capacity = newCapacity;
        }
    }

    private int nextPowerOfTwo(int num) {
        return num <= 1 ? 2 : Integer.highestOneBit(num) * 2;
    }

    public class ObjectArrayIterator {
        ObjectArray<T> array;
        int index;

        private ObjectArrayIterator(ObjectArray<T> array) {
            this.array = array;
            this.index = 0;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public boolean hasNext() {
            while (index < array.capacity && array.get(index) == null) {
                index++;
            }
            return index != array.capacity;
        }

        public T next() {
            var result = array.get(index);
            index++;
            return result;
        }

        public int getIndex() {
            return index;
        }

        public void delete() {
            array.remove(index);
            index++;
        }

        public T peek() {
            return array.get(index);
        }
    }
}
