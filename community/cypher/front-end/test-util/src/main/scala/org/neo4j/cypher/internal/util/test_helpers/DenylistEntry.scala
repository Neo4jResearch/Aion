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

import org.opencypher.tools.tck.api.Scenario

import scala.util.matching.Regex

trait DenylistEntry {
  def isDenylisted(scenario: Scenario): Boolean
  def isFlaky(scenario: Scenario): Boolean = isDenylisted(scenario) && isFlaky
  def isFlaky: Boolean
}

case class FeatureDenylistEntry(
  featureName: String,
  override val isFlaky: Boolean
) extends DenylistEntry {
  override def isDenylisted(scenario: Scenario): Boolean = scenario.featureName == featureName
  override def toString: String = s"""${if (isFlaky) "?" else ""}Feature "$featureName""""
}

case class ScenarioDenylistEntry(
  featureName: Option[String],
  scenarioName: String,
  exampleNumberOrName: Option[String],
  override val isFlaky: Boolean
) extends DenylistEntry {

  def isDenylisted(scenario: Scenario): Boolean = {
    scenarioName == scenario.name &&
    featureName.forall(_ == scenario.featureName) &&
    exampleNumberOrName.forall(_ == scenario.exampleIndex.map(_.toString).getOrElse(""))
  }

  override def toString: String = {
    val entry = featureName.map { feature =>
      val scenarioString = s"""Feature "$feature": Scenario "$scenarioName""""
      exampleNumberOrName.map { example =>
        s"""$scenarioString: Example "$example""""
      } getOrElse {
        scenarioString
      }
    } getOrElse {
      // legacy version
      scenarioName
    }
    if (isFlaky) s"?$entry" else entry
  }
}

object DenylistEntry {
  val entryPattern: Regex = """(\??)Feature "([^"]*)": Scenario "([^"]*)"(?:: Example "(.*)")?""".r
  val featurePattern: Regex = """^(\??)Feature "([^"]+)"$""".r

  def apply(line: String): DenylistEntry = {
    if (line.startsWith("?") || line.startsWith("Feature")) {
      line match {
        case entryPattern(questionMark, featureName, scenarioName, null) =>
          ScenarioDenylistEntry(Some(featureName), scenarioName, None, isFlaky = questionMark.nonEmpty)
        case entryPattern(questionMark, featureName, scenarioName, exampleNumberOrName) =>
          ScenarioDenylistEntry(
            Some(featureName),
            scenarioName,
            Some(exampleNumberOrName),
            isFlaky = questionMark.nonEmpty
          )
        case featurePattern(questionMark, featureName) =>
          FeatureDenylistEntry(featureName, isFlaky = questionMark.nonEmpty)
        case other => throw new UnsupportedOperationException(s"Could not parse denylist entry $other")
      }

    } else {
      // Legacy case of just stating the scenario
      ScenarioDenylistEntry(None, line, None, isFlaky = false)
    }
  }
}
