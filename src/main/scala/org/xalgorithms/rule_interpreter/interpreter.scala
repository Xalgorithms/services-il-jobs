package org.xalgorithms.rule_interpreter


import org.xalgorithms.rule_interpreter.udt._
import org.xalgorithms.rule_interpreter.common.{getValueByKeyString, applyOperator, setKey}
import play.api.libs.json._

object interpreter {
  def runAll(c: Context, s: Steps, active: Boolean = false): (Context, String) = {
    var res = c
    val steps = s.get

    val docId = (c.get \ "_id" \ "$oid").getOrElse(JsString("")).as[String]
    val recorder = new Recorder(docId, active)

    steps.foreach {s =>
      res = run(c, s)
      recorder.record(s.name, c, res)
    }

    val history = Json.stringify(recorder.getAll)

    (res, history)
  }

  def run(c: Context, s: Step): Context = {
    invokeInterpreter(s.name, s, c)
  }

  def invokeInterpreter(action: String, step: Step, context: Context): Context = action match {
    case "map" => interpretMap(context, step)
    case "revise" => interpretRevise(context, step)
    case "assemble" => interpretAssemble(context, step)
    case n => throw new Exception(s"Invalid action $n")
  }

  def interpretMap(context: Context, step: Step): Context = {
    val section = context.getContextSection(step.table.get)

    context.makeAssignment(section, step.assignments.get, "$")
  }

  def interpretRevise(context: Context, s: Step): Context = {
    val o = context.getContextSection(s.table.get)

    context.makeAssignment(o, s.assignments.get, "revision")
  }

  def interpretAssemble(context: Context, s: Step): Context = {
    var target = Json.obj()

    val columns = s.columns.get
    columns.foreach {c =>
      val sourceTableName = c.table
      c.sources.foreach {s =>
        val v = getColumnValue(context.get, s, sourceTableName)(target)
        target = setKey(target, s.name, JsString(v))
      }
    }

    context.setNewTable(s.table_name.get, target)
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
}
