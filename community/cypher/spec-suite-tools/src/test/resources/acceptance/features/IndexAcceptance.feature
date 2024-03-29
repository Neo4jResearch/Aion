#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

#encoding: utf-8

Feature: IndexAcceptance

  Scenario: Handling numerical literal on the left when using an index
    Given an empty graph
    And having executed:
      """
      CREATE (:Product {unitsInStock: 8})
      CREATE (:Product {unitsInStock: 12})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Product) ON (n.unitsInStock)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (p:Product)
      WHERE 10 < p.unitsInStock
      RETURN p
      """
    Then the result should be, in any order:
      | p                             |
      | (:Product {unitsInStock: 12}) |
    And no side effects

  Scenario: Handling numerical literal on the right when using an index
    Given an empty graph
    And having executed:
      """
      CREATE (:Product {unitsInStock: 8})
      CREATE (:Product {unitsInStock: 12})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Product) ON (n.unitsInStock)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (p:Product)
      WHERE p.unitsInStock > 10
      RETURN p
      """
    Then the result should be, in any order:
      | p                             |
      | (:Product {unitsInStock: 12}) |
    And no side effects

  Scenario: Works fine with index
    Given an empty graph
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.name)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MERGE (person:Person {name: 'Lasse'})
      RETURN person.name
      """
    Then the result should be, in any order:
      | person.name |
      | 'Lasse'     |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |

  Scenario: Works with indexed and unindexed property
    Given an empty graph
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.name)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MERGE (person:Person {name: 'Lasse', id: 42})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 2 |

  Scenario: Works with two indexed properties
    Given an empty graph
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.name)
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.id)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MERGE (person:Person {name: 'Lasse', id: 42})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 2 |

  Scenario: Should be able to merge using property from match with index
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {name: 'A', bornIn: 'New York'})
      CREATE (:Person {name: 'B', bornIn: 'Ohio'})
      CREATE (:Person {name: 'C', bornIn: 'New Jersey'})
      CREATE (:Person {name: 'D', bornIn: 'New York'})
      CREATE (:Person {name: 'E', bornIn: 'Ohio'})
      CREATE (:Person {name: 'F', bornIn: 'New Jersey'})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:City) ON (n.name)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (person:Person)
      MERGE (city:City {name: person.bornIn})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 3 |
      | +labels     | 1 |
      | +properties | 3 |

  Scenario: Merge with an index must properly handle multiple labels
    Given an empty graph
    And having executed:
      """
      CREATE (:L:A {prop: 42})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:L) ON (n.prop)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MERGE (test:L:B {prop: 42})
      RETURN labels(test) AS labels
      """
    Then the result should be (ignoring element order for lists):
      | labels     |
      | ['L', 'B'] |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |

  Scenario: Should allow AND and OR with index and equality predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (u:User {prop1: x, prop2: x})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:User) ON (n.prop1)
      """
    And having executed:
      """
      CREATE INDEX FOR (n:User) ON (n.prop2)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE ((c.prop1 = 1 AND c.prop2 = 1)
      OR (c.prop1 = 11 AND c.prop2 = 11))
      RETURN c
      """
    Then the result should be, in any order:
      | c                               |
      | (:User {prop1: 1, prop2: 1})    |
      | (:User {prop1: 11, prop2: 11})  |
    And no side effects


  Scenario: Should allow AND and OR with index and inequality predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (:User {prop1: x, prop2: x})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:User) ON (n.prop1)
      """
    And having executed:
      """
      CREATE INDEX FOR (n:User) ON (n.prop2)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE ((c.prop1 >= 1 AND c.prop2 < 2)
      OR (c.prop1 > 10 AND c.prop2 <= 11))
      RETURN c
      """
    Then the result should be, in any order:
      | c                               |
      | (:User {prop1: 1, prop2: 1})    |
      | (:User {prop1: 11, prop2: 11})  |
    And no side effects

  Scenario: Should allow AND and OR with index and STARTS WITH predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (:User {prop1: x+'_val', prop2: x+'_val'})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:User) ON (n.prop1)
      """
    And having executed:
      """
      CREATE INDEX FOR (n:User) ON (n.prop2)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE ((c.prop1 STARTS WITH '1_' AND c.prop2 STARTS WITH '1_')
      OR (c.prop1 STARTS WITH '11_' AND c.prop2 STARTS WITH '11_'))
      RETURN c
      """
    Then the result should be, in any order:
      | c                                           |
      | (:User {prop1: '1_val', prop2: '1_val'})    |
      | (:User {prop1: '11_val', prop2: '11_val'})  |
    And no side effects

  Scenario: Should allow AND and OR with index and regex predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (:User {prop1: x+'_val', prop2: x+'_val'})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:User) ON (n.prop1)
      """
    And having executed:
      """
      CREATE INDEX FOR (n:User) ON (n.prop2)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE ((c.prop1 =~ '1_.*' AND c.prop2 =~ '1_.*')
      OR (c.prop1 =~ '11_.*' AND c.prop2 =~ '11_.*'))
      RETURN c
      """
    Then the result should be, in any order:
      | c                                           |
      | (:User {prop1: '1_val', prop2: '1_val'})    |
      | (:User {prop1: '11_val', prop2: '11_val'})  |
    And no side effects

  Scenario: Should allow OR with index and regex predicates
    Given an empty graph
    And having executed:
      """
      UNWIND range(1, 100) AS x CREATE (u:User {prop: x+'_val'})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:User) ON (n.prop)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (c:User)
      WHERE c.prop =~ '1_.*' OR c.prop =~ '11_.*'
      RETURN c
      """
    Then the result should be, in any order:
      | c                         |
      | (:User {prop: '1_val'})   |
      | (:User {prop: '11_val'})  |
    And no side effects

  Scenario: Should allow OR with index and incoming scope
    Given an empty graph
    And having executed:
      """
      CREATE (:Person   {name: 'x', number: 0}),
             (:Person   {name: 'y', number: 1}),
             (:Person   {name: 'z', number: 2})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.name)
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.number)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      WITH 100 as variable
      MATCH (n:Person)
      WHERE n.name STARTS WITH 'x' OR n.number = 1
      RETURN variable, n.name, n.number
      """
    Then the result should be, in any order:
      | variable | n.name | n.number |
      | 100      | 'x'    | 0        |
      | 100      | 'y'    | 1        |
    And no side effects

  Scenario: Should allow OR with index and incoming scope to OPTIONAL
    Given an empty graph
    And having executed:
      """
      CREATE (:Person   {name: 'x', number: 0}),
             (:Person   {name: 'y', number: 1}),
             (:Person   {name: 'z', number: 2})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.name)
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.number)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      WITH 100 as variable
      OPTIONAL MATCH (n:Person)
      WHERE n.name STARTS WITH 'x' OR n.number = 1
      RETURN variable, n.name, n.number
      """
    Then the result should be, in any order:
      | variable | n.name | n.number |
      | 100      | 'x'    | 0        |
      | 100      | 'y'    | 1        |
    And no side effects

  Scenario: STARTS WITH should handle null prefix
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {name: 'Jack'})
      CREATE (:Person {name: 'Jill'})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.name)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (p:Person)
      WHERE p.name STARTS WITH null
      RETURN p
      """
    Then the result should be, in any order:
      | p |
    And no side effects

  Scenario: Index seek should handle null value
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {name: 'Jack'})
      CREATE (:Person {name: 'Jill'})
      """
    And having executed:
      """
      CREATE INDEX FOR (n:Person) ON (n.name)
      """
    And having executed:
      """
      CALL db.awaitIndexes()
      """
    When executing query:
      """
      MATCH (p:Person)
      WHERE p.name = null
      RETURN p
      """
    Then the result should be, in any order:
      | p                             |
    And no side effects

