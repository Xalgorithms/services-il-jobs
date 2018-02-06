package org.xalgorithms.rule_interpreter

import org.xalgorithms.rule_interpreter.udt.Step
import play.api.libs.json.{JsResult, Json}

class Steps(str: String) {
  val stepResult: JsResult[List[Step]] = Json.parse(str).validate[List[Step]]
  private[this] val s = stepResult.asOpt.orNull

  def get(): List[Step] = {
    s
  }
}
