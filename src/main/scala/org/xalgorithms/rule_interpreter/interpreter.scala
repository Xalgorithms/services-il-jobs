package org.xalgorithms.rule_interpreter


import org.xalgorithms.rule_interpreter.udt._
import org.xalgorithms.rule_interpreter.common.{getContextSection, getValueByKeyString, getVirtualTable, initRevision,
  setEmptyVirtualTable, setKey, applyOperator}
import play.api.libs.json._

object interpreter {
  def parse(c: String, s: String): JsValue = {
    println(s)
    println(Json)
    val stepResult: JsResult[Step] = Json.parse(s).validate[Step]
    val step = stepResult.asOpt.orNull

    val parsedContext = Json.parse(c).as[JsObject]
    // Create empty $ object on context
    val context = setEmptyVirtualTable(parsedContext)

    invokeParser(step.name, step, context)
  }

  def invokeParser(action: String, step: Step, context: JsValue): JsValue = action match {
    case "map" => parseMap(context, step)
    case "revise" => parseRevise(context, step)
    case "assemble" => parseAssemble(context, step)
    case n => JsString( s"Invalid action $n" )
  }

  def parseMap(context: JsValue, step: Step): JsValue = {
    val section = getContextSection(context, step.table.get)

    makeAssignment(context, section, step.assignments.get, "$")
  }

  def parseRevise(context: JsValue, s: Step): JsValue = {
    val c = initRevision(context.as[JsObject], s)
    val o = getContextSection(c, s.table.get)

    makeAssignment(c, o, s.assignments.get, "revision")
  }

  def parseAssemble(context: JsValue, s: Step): JsValue = {
    var target = Json.obj()

    val columns = s.columns.get
    columns.foreach {c =>
      val sourceTableName = c.table
      c.sources.foreach {s =>
        val v = getColumnValue(context, s, sourceTableName)(target)
        target = setKey(target, s.name, JsString(v))
      }
    }

    val newTable = setKey((context \ "table").as[JsObject], s.table_name.get, target)

    setKey(context.as[JsObject], "table", newTable)
  }

  def getColumnValue(context: JsValue, s: Source, sourceTableName: String)(implicit target: JsObject = null): String = {
    val isValidColumn = evaluateExpression(context, s.expr, sourceTableName)
    if (isValidColumn) {
      return (context \ "table" \ sourceTableName \ s.source).get.as[String]
    }

    null
  }

  def evaluateExpression(context: JsValue, exp: Expression, sourceTableName: String)(implicit target: JsObject = null): Boolean = {
    val left = getExprValue(context, exp.left, sourceTableName)
    val right = getExprValue(context, exp.right, sourceTableName)
    val op = exp.op

    applyOperator(left, right, op)
  }

  def getExprValue(context: JsValue, a: Assignment, sourceTableName: String)(implicit target: JsObject = null): String = {
    val operandType = a.`type`

    operandType match {
      case "reference" =>
        val section = a.section.get
        val sourceTable = section match {
          case "_context" => (context \ "table" \ sourceTableName).get
          case s => (context \ s).get
        }

        getValueByKeyString(sourceTable, a.key.get)

      case "number" => a.value.get
      case "string" => a.value.get
      case "name" => (target \ a.value.get).get.as[String]
      case n => s"Invalid type $n"
    }
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
