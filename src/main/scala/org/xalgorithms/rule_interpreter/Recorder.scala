package org.xalgorithms.rule_interpreter

import play.api.libs.json._
import org.xalgorithms.rule_interpreter.common.setKey

class Recorder(document_id: String, active: Boolean) {
  private var steps = Json.obj()

  def record(name: String, before: Context, after: Context): Unit = {
    if (active == false) {
      return
    }
    val records = (steps \ name)

    if (records.isEmpty) {
      val firstRecords = Json.arr(before.get, after.get)
      steps = setKey(steps, name, firstRecords)
    } else {
      val r = records.get.as[JsArray]
      r :+ after.get
      steps = setKey(steps, name, r)
    }
  }

  def getAll(): JsObject = {
    steps
  }
}
