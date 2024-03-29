/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.test_helpers

import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks.defaultSeed
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite
import org.scalatest.prop.CypherScalaTestSeedAccess
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

trait CypherScalaCheckDrivenPropertyChecks extends Suite with ScalaCheckDrivenPropertyChecks with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    setScalaCheckInitialSeed(defaultSeed.getOrElse(util.Random.nextLong()))
  }

  protected def setScalaCheckInitialSeed(seed: Long): Unit = {
    CypherScalaTestSeedAccess.setSeed(seed)
  }
}

object CypherScalaCheckDrivenPropertyChecks {
  private val defaultSeed: Option[Long] = sys.env.get("CYPHER_SCALACHECK_SEED").map(_.toLong)
}
