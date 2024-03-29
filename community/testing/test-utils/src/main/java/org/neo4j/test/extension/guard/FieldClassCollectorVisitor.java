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
package org.neo4j.test.extension.guard;

import java.util.Set;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.TypePath;

public class FieldClassCollectorVisitor extends FieldVisitor {
    private final Set<String> descriptors;
    private final AnnotationVisitor annotationVisitor;

    FieldClassCollectorVisitor(int apiVersion, Set<String> descriptors, AnnotationVisitor annotationVisitor) {
        super(apiVersion);
        this.descriptors = descriptors;
        this.annotationVisitor = annotationVisitor;
    }

    @Override
    public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
        descriptors.add(descriptor);
        return annotationVisitor;
    }

    @Override
    public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String descriptor, boolean visible) {
        descriptors.add(descriptor);
        return annotationVisitor;
    }
}
