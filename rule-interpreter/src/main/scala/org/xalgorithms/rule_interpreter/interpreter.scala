package org.xalgorithms.rule_interpreter


import org.xalgorithms.rule_interpreter.udt._
import play.api.libs.json._

object interpreter {
  def parse(c: String, s: String): JsValue = {
    val stepResult: JsResult[Step] = Json.parse(s).validate[Step]
    val step = stepResult.asOpt.getOrElse(null)

    val parsedContext = Json.parse(c).as[JsObject]
    // Create empty $ object on context
    val context = setEmpty$(parsedContext)

    invokeParser(step.name, step, context)
  }

  def invokeParser(n: String, s: Step, c: JsValue): JsValue = n match {
    case "map" => parseMap(c, s)
    case n => JsString( s"Invalid action $n" )
  }

  def parseMap(c: JsValue, s: Step): JsValue = {
    val o = getContext(c, s.table)

    makeAssignment(c, o, s.assignments)
  }

  def setEmpty$(c: JsObject): JsObject = {
    if ((c \ "$").isDefined) {
      return c
    }
    setKey(c, "$", JsObject(Seq()))
  }

  def get$(c: JsValue): JsValue = {
    (c \ "$").get
  }

  def getContext(c: JsValue, t: Table): JsValue = {
    val section = t.section
    if (section != "_virtual") {
      val key = t.key

      return (c \ section \ key).get
    }

    return null
  }

  def makeAssignment(rootContext: JsValue, context: JsValue, assignments: List[Assignment]): JsValue = {
    var res = rootContext

    assignments.foreach { a =>
      val assignmentType = a.`type`

      res = assignmentType match {
        case "reference"  => referenceAssingnment(res, context, a)
        case "string" => stringAssignment(res, context, a)
        case "number" => numberAssignment(res, context, a)
        case "function"  => functionAssignement(res, context, a)
        case _  => res
      }
    }
    res
  }

  def referenceAssingnment(root: JsValue, c: JsValue, a: Assignment): JsValue = {
    val key = a.column.get
    val value = getValueByKeyString(c, a.key.get)
    val obj = (root \ "$").get.as[JsObject]

    val $ = setKey(obj, key, JsString(value))

    setKey(root.as[JsObject], "$", $)
  }

  def stringAssignment(root: JsValue, c: JsValue, a: Assignment): JsValue = {
    val key = a.column.get
    val value = a.value.getOrElse("")
    val obj = (root \ "$").get.as[JsObject]

    val $ = setKey(obj, key, JsString(value))

    setKey(root.as[JsObject], "$", $)
  }

  def numberAssignment(root: JsValue, c: JsValue, a: Assignment): JsValue = {
    val key = a.column.get
    val value = a.value.getOrElse("")
    val obj = (root \ "$").get.as[JsObject]

    val $ = setKey(obj, key, JsNumber(value.toInt))

    setKey(root.as[JsObject], "$", $)
  }

  def functionAssignement(root: JsValue, c: JsValue, a: Assignment): JsValue = {
    val operator = a.name.get
    val current$ = get$(root)
    val value: Int = operator match {
      case "add"  => mathOperators.invokeOperator(a, current$)
      case "multiply" => mathOperators.invokeOperator(a, current$)
      case _  => 0
    }
    val key = a.column.get
    val obj = (root \ "$").get.as[JsObject]

    val $ = setKey(obj, key, JsNumber(value))

    setKey(root.as[JsObject], "$", $)
  }



  def setKey(o: JsObject, key: String, value: JsValue): JsObject = {
    val m = Map(key -> value)
    o ++ Json.toJson(m).as[JsObject]
  }

  def getValueByKeyString(o: JsValue, k: String): String = {
    val parts = k.split("\\.")
    getValueByKeys(o, parts)
  }

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
}
