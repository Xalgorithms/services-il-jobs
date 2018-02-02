package org.xalgorithms.rule_interpreter

import org.xalgorithms.rule_interpreter.common.getValueByKeyString
import org.xalgorithms.rule_interpreter.common.setKey
import org.xalgorithms.rule_interpreter.udt.{Assignment, Table}
import play.api.libs.json._


/**
  * Context is a data in json format, which on input looks like
  * {
  *   "tables": {
  *     "items": ... document items json ...
  *     ... any other relevant tables ...
  *   }
  * }
  *
  * On output it looks like:
  * {
  *   "tables": {
  *     "items": ... document items json ...
  *     ... any other relevant tables ...
  *   },
  *   "$": {
  *     ... all virtual tables (results of intermediate operations) ...
  *   },
  *   "revision": {
  *     ... sequence of changes on the document ...
  *   }
  * }
  *
  */
class Context(s: String = "") {
  private[this] var c: JsObject = Json.parse(s).as[JsObject]

  c = setEmptyVirtualTable()
  c = setEmptyRevision()

  // Set empty $ object on context if doesn't exist
  private[this] def setEmptyVirtualTable(): JsObject = {
    if ((c \ "$").isDefined) {
      return c
    }
    c = setKey(c, "$", Json.obj())
    c
  }

  // Get $ object from context
  private[this] def getVirtualTable(c: JsValue): JsValue = {
    (c \ "$").get
  }

  // Set empty 'revision' object on context if doesn't exist
  private[this] def setEmptyRevision(): JsObject = {
    if ((c \ "revision").isDefined) {
      return c
    }
    c = setKey(c, "revision", Json.obj())
    c
  }

  def get(): JsObject = {
    c
  }

  // Get specific section in context (e.g. table:items) or virtual table
  def getContextSection(t: Table): JsValue = {
    val section = t.section
    if (section != "_virtual") {
      val key = t.key

      return (c \ section \ key).get
    }

    getVirtualTable(c)
  }

  def makeAssignment(section: JsValue, assignments: List[Assignment], target: String): Context = {
    var res = c.as[JsValue]

    assignments.foreach { a =>
      val assignmentType = a.`type`

      res = assignmentType match {
        case "reference"  => referenceAssignment(res, section, a, target)
        case "string" => stringAssignment(res, a, target)
        case "number" => numberAssignment(res, a, target)
        case "function"  => functionAssignment(res, section, a, target)
        case _  => res
      }
    }
    c = res.as[JsObject]

    this
  }

  def setNewTable(tableName: String, target: JsObject): Context = {
    val newTable = setKey((c \ "table").as[JsObject], tableName, target)

    c = setKey(c, "table", newTable)

    this
  }

  def referenceAssignment(context: JsValue, section: JsValue, a: Assignment, target: String): JsValue = {
    val key = a.column.get
    val value = getValueByKeyString(section, a.key.get)
    val obj = (context \ target).get.as[JsObject]

    val res = setKey(obj, key, JsString(value))

    setKey(context.as[JsObject], target, res)
  }

  def stringAssignment(context: JsValue, a: Assignment, target: String): JsValue = {
    val key = a.column.get
    val value = a.value.getOrElse("")
    val obj = (context \ target).get.as[JsObject]

    val res = setKey(obj, key, JsString(value))

    setKey(context.as[JsObject], target, res)
  }

  def numberAssignment(context: JsValue, a: Assignment, target: String): JsValue = {
    val key = a.column.get
    val value = a.value.getOrElse("")
    val obj = (context \ target).get.as[JsObject]

    val res = setKey(obj, key, JsNumber(value.toInt))

    setKey(context.as[JsObject], target, res)
  }

  def functionAssignment(context: JsValue, section: JsValue, a: Assignment, target: String): JsValue = {
    val operator = a.name.get
    val current$ = getVirtualTable(context)
    val value: Int = operator match {
      case "add"  => mathOperators.invokeOperator(a, current$)
      case "multiply" => mathOperators.invokeOperator(a, current$)
      case _  => 0
    }
    val key = a.column.get
    val obj = (context \ target).get.as[JsObject]

    val res = setKey(obj, key, JsNumber(value))

    setKey(context.as[JsObject], target, res)
  }
}
