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
package org.neo4j.collection.trackable;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.IteratorState;
import org.neo4j.memory.MemoryTracker;

@SuppressWarnings({"unchecked", "NullableProblems"})
public final class HeapTrackingConcurrentHashSet<E> extends AbstractHeapTrackingConcurrentHash
        implements Set<E>, AutoCloseable {
    private static final long SHALLOW_SIZE_THIS = shallowSizeOfInstance(HeapTrackingConcurrentHashSet.class);

    private HeapTrackingConcurrentHashSet(MemoryTracker memoryTracker) {
        super(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    private HeapTrackingConcurrentHashSet(MemoryTracker memoryTracker, int initialCapacity) {
        super(memoryTracker, initialCapacity);
    }

    public static <E> HeapTrackingConcurrentHashSet<E> newSet(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentHashSet<>(memoryTracker);
    }

    public static <E> HeapTrackingConcurrentHashSet<E> newSet(MemoryTracker memoryTracker, int size) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentHashSet<>(memoryTracker, size);
    }

    @Override
    public boolean add(E value) {
        int hash = this.hash(value);
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            Node<E> newNode = new Node<>(value, null);
            this.addToSize(1);
            if (currentArray.compareAndSet(index, null, newNode)) {
                return true;
            }
            addToSize(-1);
        }
        return slowAdd(value, hash, currentArray);
    }

    private boolean slowAdd(E value, int hash, AtomicReferenceArray<Object> currentArray) {
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node<E> e = (Node<E>) o;
                while (e != null) {
                    Object candidate = e.value;
                    if (candidate.equals(value)) {
                        return false;
                    }
                    e = e.getNext();
                }
                Node<E> newNode = new Node<>(value, (Node<E>) o);
                if (currentArray.compareAndSet(index, o, newNode)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return true;
                }
            }
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new HashIterator<>();
    }

    @Override
    public Object[] toArray() {
        ArrayList<E> arrayList = new ArrayList<>(this);
        return arrayList.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        ArrayList<T> arrayList = new ArrayList<>();
        for (E e : this) {
            arrayList.add((T) e);
        }
        return arrayList.toArray(a);
    }

    @Override
    void transfer(AtomicReferenceArray<Object> src, ResizeContainer resizeContainer) {
        AtomicReferenceArray<Object> dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length() - 1; ) {
            Object o = src.get(j);
            if (o == null) {
                if (src.compareAndSet(j, null, RESIZED)) {
                    j++;
                }
            } else if (o == RESIZED || o == RESIZING) {
                j = (j & -ResizeContainer.QUEUE_INCREMENT) + ResizeContainer.QUEUE_INCREMENT;
                if (resizeContainer.resizers.get() == 1) {
                    break;
                }
            } else {
                Node<E> e = (Node<E>) o;
                if (src.compareAndSet(j, o, RESIZING)) {
                    while (e != null) {
                        this.unconditionalCopy(dest, e);
                        e = e.getNext();
                    }
                    src.set(j, RESIZED);
                    j++;
                }
            }
        }
        resizeContainer.decrementResizerAndNotify();
        resizeContainer.waitForAllResizers();
    }

    @Override
    void reverseTransfer(AtomicReferenceArray<Object> src, ResizeContainer resizeContainer) {
        AtomicReferenceArray<Object> dest = resizeContainer.nextArray;
        while (resizeContainer.getQueuePosition() > 0) {
            int start = resizeContainer.subtractAndGetQueuePosition();
            int end = start + ResizeContainer.QUEUE_INCREMENT;
            if (end > 0) {
                if (start < 0) {
                    start = 0;
                }
                for (int j = end - 1; j >= start; ) {
                    Object o = src.get(j);
                    if (o == null) {
                        if (src.compareAndSet(j, null, RESIZED)) {
                            j--;
                        }
                    } else if (o == RESIZED || o == RESIZING) {
                        resizeContainer.zeroOutQueuePosition();
                        return;
                    } else {
                        Node<E> e = (Node<E>) o;
                        if (src.compareAndSet(j, o, RESIZING)) {
                            while (e != null) {
                                this.unconditionalCopy(dest, e);
                                e = e.getNext();
                            }
                            src.set(j, RESIZED);
                            j--;
                        }
                    }
                }
            }
        }
    }

    private void unconditionalCopy(AtomicReferenceArray<Object> dest, Node<E> toCopyNode) {
        int hash = this.hash(toCopyNode.value);
        AtomicReferenceArray<Object> currentArray = dest;
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = ((ResizeContainer) currentArray.get(length - 1)).nextArray;
            } else {
                Node<E> newNode;
                if (o == null) {
                    if (toCopyNode.getNext() == null) {
                        newNode = toCopyNode; // no need to duplicate
                    } else {
                        newNode = new Node<>(toCopyNode.value);
                    }
                } else {
                    newNode = new Node<>(toCopyNode.value, (Node<E>) o);
                }
                if (currentArray.compareAndSet(index, o, newNode)) {
                    return;
                }
            }
        }
    }

    @Override
    public boolean contains(Object value) {
        int hash = this.hash(value);
        AtomicReferenceArray<Object> currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node<E> e = (Node<E>) o;
                while (e != null) {
                    Object candidate = e.value;
                    if (candidate.equals(value)) {
                        return true;
                    }
                    e = e.getNext();
                }
                return false;
            }
        }
    }

    @Override
    public void clear() {
        AtomicReferenceArray<Object> currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++) {
                Object o = currentArray.get(i);
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                } else if (o != null) {
                    Node<E> e = (Node<E>) o;
                    if (currentArray.compareAndSet(i, o, null)) {
                        int removedEntries = 0;
                        while (e != null) {
                            removedEntries++;
                            e = e.getNext();
                        }
                        this.addToSize(-removedEntries);
                    }
                }
            }
            if (resizeContainer != null) {
                if (resizeContainer.isNotDone()) {
                    this.helpWithResize(currentArray);
                    resizeContainer.waitForAllResizers();
                }
                currentArray = resizeContainer.nextArray;
            }
        } while (resizeContainer != null);
    }

    @Override
    public boolean remove(Object value) {
        int hash = this.hash(value);
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == RESIZED || o == RESIZING) {
            return this.slowRemove(value, hash, currentArray);
        }
        Node<E> e = (Node<E>) o;
        while (e != null) {
            Object candidate = e.value;
            if (candidate.equals(value)) {
                Node<E> replacement = this.createReplacementChainForRemoval((Node<E>) o, e);
                if (currentArray.compareAndSet(index, o, replacement)) {
                    this.addToSize(-1);
                    return true;
                }
                return this.slowRemove(value, hash, currentArray);
            }
            e = e.getNext();
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object e : c) if (!contains(e)) return false;
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean modified = false;
        for (E e : c) if (add(e)) modified = true;
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);
        boolean modified = false;
        Iterator<E> it = iterator();
        while (it.hasNext()) {
            if (!c.contains(it.next())) {
                it.remove();
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        Objects.requireNonNull(c);
        boolean modified = false;
        Iterator<?> it = iterator();
        while (it.hasNext()) {
            if (c.contains(it.next())) {
                it.remove();
                modified = true;
            }
        }
        return modified;
    }

    private boolean slowRemove(Object value, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node<E> e = (Node<E>) o;
                while (e != null) {
                    Object candidate = e.value;
                    if (candidate.equals(value)) {
                        Node<E> replacement = this.createReplacementChainForRemoval((Node<E>) o, e);
                        if (currentArray.compareAndSet(index, o, replacement)) {
                            this.addToSize(-1);
                            return true;
                        }
                        //noinspection ContinueStatementWithLabel
                        continue outer;
                    }
                    e = e.getNext();
                }
                return false;
            }
        }
    }

    private Node<E> createReplacementChainForRemoval(Node<E> original, Node<E> toRemove) {
        if (original == toRemove) {
            return original.getNext();
        }
        Node<E> replacement = null;
        Node<E> e = original;
        while (e != null) {
            if (e != toRemove) {
                replacement = new Node<>(e.value, replacement);
            }
            e = e.getNext();
        }
        return replacement;
    }

    @Override
    public int hashCode() {
        int h = 0;
        AtomicReferenceArray<Object> currentArray = this.table;
        for (int i = 0; i < currentArray.length() - 1; i++) {
            Object o = currentArray.get(i);
            if (o == RESIZED || o == RESIZING) {
                throw new ConcurrentModificationException("can't compute hashcode while resizing!");
            }
            Node<E> e = (Node<E>) o;
            while (e != null) {
                Object value = e.value;
                h += (value == null ? 0 : value.hashCode());
                e = e.getNext();
            }
        }
        return h;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Set)) {
            return false;
        }
        Set<E> s = (Set<E>) o;
        if (s.size() != this.size()) {
            return false;
        }
        for (E e : this) {
            if (!s.contains(e)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        memoryTracker.releaseHeap(SHALLOW_SIZE_THIS);
        releaseHeap();
    }

    private class HashIterator<T> implements Iterator<T> {
        private List<IteratorState> todo;
        private IteratorState currentState;
        private Node<T> next;
        private int index;

        protected HashIterator() {
            this.currentState = new IteratorState(HeapTrackingConcurrentHashSet.this.table);
            this.findNext();
        }

        private void findNext() {
            while (this.index < this.currentState.end) {
                Object o = this.currentState.currentTable.get(this.index);
                if (o == RESIZED || o == RESIZING) {
                    AtomicReferenceArray<Object> nextArray =
                            HeapTrackingConcurrentHashSet.this.helpWithResizeWhileCurrentIndex(
                                    this.currentState.currentTable, this.index);
                    int endResized = this.index + 1;
                    while (endResized < this.currentState.end) {
                        if (this.currentState.currentTable.get(endResized) != RESIZED) {
                            break;
                        }
                        endResized++;
                    }
                    if (this.todo == null) {
                        this.todo = new FastList<>(4);
                    }
                    if (endResized < this.currentState.end) {
                        this.todo.add(
                                new IteratorState(this.currentState.currentTable, endResized, this.currentState.end));
                    }
                    int powerTwoLength = this.currentState.currentTable.length() - 1;
                    this.todo.add(
                            new IteratorState(nextArray, this.index + powerTwoLength, endResized + powerTwoLength));
                    this.currentState.currentTable = nextArray;
                    this.currentState.end = endResized;
                    this.currentState.start = this.index;
                } else if (o != null) {
                    this.next = (Node<T>) o;
                    this.index++;
                    break;
                } else {
                    this.index++;
                }
            }
            if (this.next == null && this.index == this.currentState.end && this.todo != null && !this.todo.isEmpty()) {
                this.currentState = this.todo.remove(this.todo.size() - 1);
                this.index = this.currentState.start;
                this.findNext();
            }
        }

        @Override
        public final boolean hasNext() {
            return this.next != null;
        }

        @Override
        public final T next() {
            Node<T> e = this.next;
            if (e == null) {
                throw new NoSuchElementException();
            }

            if ((this.next = e.getNext()) == null) {
                this.findNext();
            }
            return e.value;
        }
    }

    public static final class Node<T> {
        private final T value;
        private final Node<T> next;

        private Node(T value) {
            this.value = value;
            this.next = null;
        }

        private Node(T value, Node<T> next) {
            this.value = value;
            this.next = next;
        }

        public Node<T> getNext() {
            return this.next;
        }
    }
}
