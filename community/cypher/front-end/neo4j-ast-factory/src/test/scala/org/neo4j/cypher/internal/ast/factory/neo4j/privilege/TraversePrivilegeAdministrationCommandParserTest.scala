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
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase

class TraversePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: noResourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.TraverseAction, List(ast.HomeGraphScope()(_)))(pos),
              List(ast.ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH NODE A $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.TraverseAction, List(ast.HomeGraphScope()(_)))(pos),
              List(labelQualifierA),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH RELATIONSHIP * $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.TraverseAction, List(ast.HomeGraphScope()(_)))(pos),
              List(ast.RelationshipAllQualifier() _),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH ELEMENT A $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.TraverseAction, List(ast.HomeGraphScope()(_)))(pos),
              List(elemQualifierA),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.TraverseAction, List(ast.DefaultGraphScope()(_)))(pos),
              List(ast.ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH NODE A $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.TraverseAction, List(ast.DefaultGraphScope()(_)))(pos),
              List(labelQualifierA),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.TraverseAction, List(ast.DefaultGraphScope()(_)))(pos),
              List(ast.RelationshipAllQualifier() _),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH ELEMENT A $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.TraverseAction, List(ast.DefaultGraphScope()(_)))(pos),
              List(elemQualifierA),
              Seq(literalRole),
              immutable
            ))
          }

          Seq("GRAPH", "GRAPHS").foreach {
            graphKeyword =>
              test(s"$verb$immutableString TRAVERSE ON $graphKeyword * $preposition $$role") {
                yields(func(
                  ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                  List(ast.ElementsAllQualifier() _),
                  Seq(paramRole),
                  immutable
                ))
              }

              test(s"$verb$immutableString TRAVERSE ON $graphKeyword foo $preposition role") {
                yields(func(
                  ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                  List(ast.ElementsAllQualifier() _),
                  Seq(literalRole),
                  immutable
                ))
              }

              test(s"$verb$immutableString TRAVERSE ON $graphKeyword $$foo $preposition role") {
                yields(func(
                  ast.GraphPrivilege(ast.TraverseAction, List(graphScopeParamFoo))(pos),
                  List(ast.ElementsAllQualifier() _),
                  Seq(literalRole),
                  immutable
                ))
              }

              Seq("NODE", "NODES").foreach {
                nodeKeyword =>
                  test(s"validExpressions $verb$immutableString $graphKeyword $nodeKeyword $preposition") {
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(ast.LabelAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword * (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(ast.LabelAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword A $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(labelQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(labelQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword `*` $nodeKeyword A $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("*")) _))(pos),
                        List(labelQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(ast.LabelAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword * (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(ast.LabelAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(labelQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(labelQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role1, $$role2"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(labelQualifierA),
                        Seq(literalRole1, paramRole2),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword `2foo` $nodeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("2foo")) _))(pos),
                        List(labelQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition `r:ole`"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(labelQualifierA),
                        Seq(literalRColonOle),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword `A B` (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(ast.LabelQualifier("A B") _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(labelQualifierA, labelQualifierB),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role1, role2"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(labelQualifierA, labelQualifierB),
                        Seq(literalRole1, literalRole2),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo, graphScopeBaz))(pos),
                        List(labelQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                  }

                  test(s"traverseParsingErrors $verb$immutableString $graphKeyword $nodeKeyword $preposition") {
                    assertFails(s"$verb$immutableString TRAVERSE $graphKeyword * $nodeKeyword * (*) $preposition role")
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A B (*) $preposition role"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (foo) $preposition role"
                    )
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword * $preposition role")
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword A $preposition role")
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword * (*) $preposition role")
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword A (*) $preposition role")
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition r:ole"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword 2foo $nodeKeyword A (*) $preposition role"
                    )
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword * (*)")
                  }
              }

              Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
                relTypeKeyword =>
                  test(s"validExpressions $verb$immutableString $graphKeyword $relTypeKeyword $preposition") {
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(ast.RelationshipAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword * (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(ast.RelationshipAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword A $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(relQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(relQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword `*` $relTypeKeyword A $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("*")) _))(pos),
                        List(relQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(ast.RelationshipAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword * (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(ast.RelationshipAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(relQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(relQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition $$role1, role2"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(relQualifierA),
                        Seq(paramRole1, literalRole2),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword `2foo` $relTypeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("2foo")) _))(pos),
                        List(relQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition `r:ole`"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(relQualifierA),
                        Seq(literalRColonOle),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword `A B` (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(ast.RelationshipQualifier("A B") _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(relQualifierA, relQualifierB),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role1, role2"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(relQualifierA, relQualifierB),
                        Seq(literalRole1, literalRole2),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo, graphScopeBaz))(pos),
                        List(relQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                  }

                  test(s"traverseParsingErrors$verb$immutableString $graphKeyword $relTypeKeyword $preposition") {
                    assertFails(
                      s"$verb$immutableString TRAVERSE $graphKeyword * $relTypeKeyword * (*) $preposition role"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A B (*) $preposition role"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (foo) $preposition role"
                    )
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword * $preposition role")
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword A $preposition role")
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword * (*) $preposition role"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword A (*) $preposition role"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition r:ole"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword 2foo $relTypeKeyword A (*) $preposition role"
                    )
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword * (*)")
                  }
              }

              Seq("ELEMENT", "ELEMENTS").foreach {
                elementKeyword =>
                  test(s"validExpressions $verb$immutableString $graphKeyword $elementKeyword $preposition") {
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(ast.ElementsAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword * (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(ast.ElementsAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword A $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(elemQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos),
                        List(elemQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword `*` $elementKeyword A $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("*")) _))(pos),
                        List(elemQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(ast.ElementsAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword * (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(ast.ElementsAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(elemQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(elemQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role1, role2"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(elemQualifierA),
                        Seq(literalRole1, literalRole2),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword `2foo` $elementKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("2foo")) _))(pos),
                        List(elemQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition `r:ole`"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(elemQualifierA),
                        Seq(literalRColonOle),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword `A B` (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(ast.ElementQualifier("A B") _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(elemQualifierA, elemQualifierB),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition $$role1, $$role2"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos),
                        List(elemQualifierA, elemQualifierB),
                        Seq(paramRole1, paramRole2),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role"
                    )
                    func(
                      ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo, graphScopeBaz))(pos),
                      List(elemQualifierA),
                      Seq(paramRole),
                      immutable
                    )
                  }

                  test(s"traverseParsingErrors $verb$immutableString $graphKeyword $elementKeyword $preposition") {
                    assertFails(
                      s"$verb$immutableString TRAVERSE $graphKeyword * $elementKeyword * (*) $preposition role"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A B (*) $preposition role"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (foo) $preposition role"
                    )
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword * $preposition role")
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword A $preposition role")
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword * (*) $preposition role"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword A (*) $preposition role"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition r:ole"
                    )
                    assertFails(
                      s"$verb$immutableString TRAVERSE ON $graphKeyword 2foo $elementKeyword A (*) $preposition role"
                    )
                    assertFails(s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword * (*)")
                  }
              }
          }

          // Mix of specific graph and *

          test(s"$verb$immutableString TRAVERSE ON GRAPH foo, * $preposition role") {
            failsToParse
          }

          test(s"$verb$immutableString TRAVERSE ON GRAPH *, foo $preposition role") {
            failsToParse
          }

          // Database instead of graph keyword

          test(s"$verb$immutableString TRAVERSE ON DATABASES * $preposition role") {
            val offset = verb.length + immutableString.length + 13
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb$immutableString TRAVERSE ON DATABASE foo $preposition role") {
            val offset = verb.length + immutableString.length + 13
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb$immutableString TRAVERSE ON HOME DATABASE $preposition role") {
            failsToParse
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT DATABASE $preposition role") {
            failsToParse
          }
      }
  }
}
