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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class LabelMap {
    private String[] labels;
    private boolean[] present;
    private int labelSize;

    private static final List<Pair<String, Boolean>> EMPTY_LIST = List.of();

    public LabelMap() {
        labelSize = 0;
        labels = new String[labelSize];
        present = new boolean[labelSize];
    }

    public void put(String label, boolean isPresent) {
        // Search if it already exists
        for (String s : labels) {
            if (s.equals(label)) {
                return;
            }
        }
        // Otherwise add it in the end
        resizeProperties();
        labels[labelSize] = label;
        present[labelSize] = isPresent;
        labelSize++;
    }

    public void putAll(LabelMap otherList) {
        for (int i = 0; i < otherList.size(); ++i) {
            put(otherList.labels[i], otherList.present[i]);
        }
    }

    public void removeDeleted() {
        for (int i = labelSize - 1; i >= 0; --i) {
            if (!present[i]) {
                if (i + 1 < labelSize) {
                    for (int j = i + 1; j < labelSize; j++) {
                        labels[j - 1] = labels[j];
                        present[j - 1] = present[j];
                    }
                }
                labelSize--;
            }
        }
    }

    public void clear() {
        labelSize = 0;
        labels = new String[labelSize];
        present = new boolean[labelSize];
    }

    public List<String> getLabelsAsList() {
        return Arrays.asList(labels);
    }

    public List<Pair<String, Boolean>> getLabels() {
        if (labelSize == 0) {
            return EMPTY_LIST;
        }
        List<Pair<String, Boolean>> result = new ArrayList<>();
        for (int i = 0; i < labelSize; ++i) {
            result.add(ImmutablePair.of(labels[i], present[i]));
        }
        return result;
    }

    public int size() {
        return labelSize;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(labels) ^ Objects.hashCode(present) ^ Objects.hashCode(labelSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final LabelMap other = (LabelMap) obj;
        if (other.size() != this.size()) {
            return false;
        }

        for (int i = 0; i < this.size(); ++i) {
            if (!exists(other.labels[i], other.present[i])) {
                return false;
            }
        }
        return true;
    }

    private boolean exists(String label, boolean isPresent) {
        for (int i = 0; i < labelSize; ++i) {
            var value = labels[i];
            if (value.equals(label)) {
                return (present[i] == isPresent);
            }
        }
        return false;
    }

    private void resizeProperties() {
        if (labelSize + 1 >= labels.length) {
            var newSize = Math.max(2 * labelSize, 1);
            var newL = new String[newSize];
            System.arraycopy(labels, 0, newL, 0, labels.length);
            labels = newL;

            var newD = new boolean[newSize];
            System.arraycopy(present, 0, newD, 0, present.length);
            present = newD;
        }
    }
}
