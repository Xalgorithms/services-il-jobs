package org.xalgorithms.rule_interpreter

import org.xalgorithms.rule_interpreter.udt.Assignment
import play.api.libs.json.{JsObject, JsString, JsValue}

object mathOperators {
  def invokeOperator(e: Assignment, c: JsValue = null): Int = {
    // context represents $ on root context
    implicit val context: JsValue = c
    runOperator(e)
  }

  def runOperator(e: Assignment)(implicit context: JsValue): Int = {
    val operator = e.name.get
    val args = e.args.get

    val r = operator match {
      case "add" => add(args(0), args(1))
      case "multiply" => multiply(args(0), args(1))
      case _  => 0
    }

    r
  }

  def parseOperands(left: Assignment, right: Assignment)(implicit context: JsValue): (Int, Int) = {
    val leftType = left.`type`
    val rightType = right.`type`

    val l: Int = leftType match {
      case "number" => left.value.get.toInt
      case "function" => runOperator(left)
      case "reference" => getValueFromContext(left)
      case _ => 0
    }

    val r: Int = rightType match {
      case "number" => right.value.get.toInt
      case "function" => runOperator(right)
      case "reference" => getValueFromContext(right)
      case _ => 0
    }

    (l, r)
  }

  def getValueFromContext(a: Assignment)(implicit context: JsValue): Int = {
    if (a.section.get == "_context") {
      return (context.as[JsObject] \ a.key.get).as[JsString].value.toInt
    }

    return 0
  }

  def add(left: Assignment, right: Assignment)(implicit context: JsValue): Int = {
    val (l, r) = parseOperands(left, right)

    l + r
  }

  def multiply(left: Assignment, right: Assignment)(implicit context: JsValue): Int = {
    val (l, r) = parseOperands(left, right)

    l * r
  }
}
