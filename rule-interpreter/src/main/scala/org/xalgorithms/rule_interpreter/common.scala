package org.xalgorithms.rule_interpreter

import org.xalgorithms.rule_interpreter.udt.{Step, Table}
import play.api.libs.json._


object common {
  // Get $ object from context
  def getVirtualTable(c: JsValue): JsValue = {
    (c \ "$").get
  }

  // Set empty $ object on context if doesn't exist
  def setEmptyVirtualTable(c: JsObject): JsObject = {
    if ((c \ "$").isDefined) {
      return c
    }
    setKey(c, "$", Json.obj())
  }

  // Create "revision" on context with copy of the data
  def initRevision(c: JsObject, s: Step): JsObject = {
    val section = getContextSection(c, s.table.get)
    setKey(c, "revision", section)
  }

  // Set key value by key
  def setKey(o: JsObject, key: String, value: JsValue): JsObject = {
    val m = Map(key -> value)
    o ++ Json.toJson(m).as[JsObject]
  }

  // Get specific section in context (e.g. table:items) or virtual table
  def getContextSection(context: JsValue, t: Table): JsValue = {
    val section = t.section
    if (section != "_virtual") {
      val key = t.key

      return (context \ section \ key).get
    }

    getVirtualTable(context)
  }

  // Extract the value of comma separated path
  def getValueByKeyString(o: JsValue, k: String): String = {
    val parts = k.split("\\.")
    getValueByKeys(o, parts)
  }

  // Extract value by sequence of keys
  def getValueByKeys(source: JsValue, path: Array[String]): String = {
    val p = path.head
    val next = (source \ p).getOrElse(null)

    if (next == null) {
      return null
    }

    if (path.length == 1) {
      return next.as[String]
    }

    getValueByKeys(next, path.drop(1))
  }

  def applyOperator(x: String, y: String, operator: String): Boolean = operator match {
    case "eq" => x == y
    case "neq" => x != y
    case "lt" => x < y
    case "lte" => x <= y
    case "gt" => x > y
    case "gte" => x >= y
  }
}
