package org.xalgorithms.rule_interpreter


import org.xalgorithms.rule_interpreter.udt._
import org.xalgorithms.rule_interpreter.common.{setEmptyVirtualTable, getVirtualTable, setKey, getValueByKeyString, getContextSection, initRevision}
import play.api.libs.json._

object interpreter {
  def parse(c: String, s: String): JsValue = {
    val stepResult: JsResult[Step] = Json.parse(s).validate[Step]
    val step = stepResult.asOpt.orNull

    val parsedContext = Json.parse(c).as[JsObject]
    // Create empty $ object on context
    val contextWithVirtualTable = setEmptyVirtualTable(parsedContext)
    val context = initRevision(contextWithVirtualTable, step)

    invokeParser(step.name, step, context)
  }

  def invokeParser(action: String, step: Step, context: JsValue): JsValue = action match {
    case "map" => parseMap(context, step)
    case "revise" => parseRevise(context, step)
    case n => JsString( s"Invalid action $n" )
  }

  def parseMap(context: JsValue, step: Step): JsValue = {
    val section = getContextSection(context, step.table)

    makeAssignment(context, section, step.assignments, "$")
  }

  def parseRevise(context: JsValue, s: Step): JsValue = {
    val o = getContextSection(context, s.table)
    makeAssignment(context, o, s.assignments, "revision")
  }

  def makeAssignment(context: JsValue, section: JsValue, assignments: List[Assignment], target: String): JsValue = {
    var res = context

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
    res
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
