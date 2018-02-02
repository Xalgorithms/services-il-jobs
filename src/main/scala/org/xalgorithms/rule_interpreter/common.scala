package org.xalgorithms.rule_interpreter

import play.api.libs.json._


object common {
  // Set key value by key
  def setKey(o: JsObject, key: String, value: JsValue): JsObject = {
    o ++ Json.obj(key -> value)
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

  // Transform string to math operation
  def applyOperator(x: String, y: String, operator: String): Boolean = operator match {
    case "eq" => x == y
    case "neq" => x != y
    case "lt" => x < y
    case "lte" => x <= y
    case "gt" => x > y
    case "gte" => x >= y
  }

  def recursiveSetKeys(o: JsValue, keys: String, value: JsValue): JsValue = {
    val parts = keys.split("\\.")

    recursiveSetValueByKeys(o, parts, value)
  }

  def recursiveSetValueByKeys(source: JsValue, path: Array[String], value: JsValue): JsValue = {
    val p = path.head
    val next = (source \ p).getOrElse(JsNull)

    if (next == JsNull && path.length != 1) {
      val emptyObj = Json.obj()

      val res = recursiveSetValueByKeys(emptyObj, path.drop(1), value)
      return setKey(source.as[JsObject], p, res)
    }

    if (path.length == 1) {
      return setKey(source.as[JsObject], p, value)
    }

    val res = recursiveSetValueByKeys(next, path.drop(1), value)
    setKey(source.as[JsObject], p, res)
  }
}
