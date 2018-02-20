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
    val beforeRenamed = renameVirtualTable(before.get)
    val afterRenamed = renameVirtualTable(after.get)

    if (records.isEmpty) {
      val firstRecords = Json.arr(beforeRenamed, afterRenamed)
      steps = setKey(steps, name, firstRecords)
    } else {
      val r = records.get.as[JsArray]
      r :+ afterRenamed
      steps = setKey(steps, name, r)
    }
  }

  def getAll(): JsObject = {
    steps
  }

  // Rename '$', since it's not valid key in BSON document
  def renameVirtualTable(r: JsObject): JsObject = {
    val vt = (r \ "$").get
    val o = r - "$"
    setKey(o, "_virtual", vt)
  }
}
