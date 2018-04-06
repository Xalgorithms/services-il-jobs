package org.xalgorithms.rules

import org.xalgorithms.rules.elements._

import play.api.libs.json._

abstract class LoadTableSource {
  def read(ptref: PackagedTableReference): JsValue

  def load(ptref: PackagedTableReference): Seq[Map[String, Value]] = {
    val jsa = read(ptref).asInstanceOf[JsArray]
    return jsa.value.map { v =>
      val o = v.validate[JsObject].getOrElse(null)
      o.fields.map { tup => (tup._1, make(tup._2)) }.toMap
    }
  }

  def make(value: JsValue): Value = {
    val n = value.validate[JsNumber].getOrElse(null)
    if (null != n) {
      return new NumberValue(n.value)
    }

    return new StringValue(value.validate[String].getOrElse(null))
  }
}
