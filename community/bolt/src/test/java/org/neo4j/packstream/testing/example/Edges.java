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
package org.neo4j.packstream.testing.example;

import static org.neo4j.packstream.testing.example.Nodes.ALICE;
import static org.neo4j.packstream.testing.example.Nodes.BOB;
import static org.neo4j.packstream.testing.example.Nodes.CAROL;
import static org.neo4j.packstream.testing.example.Nodes.DAVE;
import static org.neo4j.packstream.testing.example.Support.NO_PROPERTIES;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.map;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.RelationshipValue;

public final class Edges {

    // Relationship types
    public static final TextValue KNOWS = stringValue("KNOWS");
    public static final TextValue LIKES = stringValue("LIKES");
    public static final TextValue DISLIKES = stringValue("DISLIKES");
    public static final TextValue MARRIED_TO = stringValue("MARRIED_TO");
    public static final TextValue WORKS_FOR = stringValue("WORKS_FOR");

    // Edges
    public static final RelationshipValue ALICE_KNOWS_BOB = relationshipValue(
            12L, "r1", ALICE, BOB, KNOWS, map(new String[] {"since"}, new AnyValue[] {longValue(1999L)}));
    public static final RelationshipValue ALICE_LIKES_CAROL =
            relationshipValue(13L, "r2", ALICE, CAROL, LIKES, NO_PROPERTIES);
    public static final RelationshipValue CAROL_DISLIKES_BOB =
            relationshipValue(32L, "r3", CAROL, BOB, DISLIKES, NO_PROPERTIES);
    public static final RelationshipValue CAROL_MARRIED_TO_DAVE =
            relationshipValue(34L, "r4", CAROL, DAVE, MARRIED_TO, NO_PROPERTIES);
    public static final RelationshipValue DAVE_WORKS_FOR_DAVE =
            relationshipValue(44L, "r5", DAVE, DAVE, WORKS_FOR, NO_PROPERTIES);

    private Edges() {}
}
