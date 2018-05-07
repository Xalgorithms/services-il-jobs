package org.xalgorithms.rules.elements

abstract class Value {
  def matches(v: Value, op: String): Boolean
}

class NumberValue(val value: BigDecimal) extends Value {
  def matches(v: Value, op: String): Boolean = v match {
    case (sv: StringValue) => matches_value(BigDecimal(sv.value), op)
    case (nv: NumberValue) => matches_value(nv.value, op)
    case _ => false
  }

  def matches_value(v: BigDecimal, op: String): Boolean = op match {
    case "eq" => value == v
    case "lt" => value < v
    case "lte" => value <= v
    case "gt" => value > v
    case "gte" => value >= v
    case _ => false
  }
}

class StringValue(val value: String) extends Value {
  def matches(v: Value, op: String): Boolean = v match {
    case (sv: StringValue) => matches_value(sv.value, op)
    case (nv: NumberValue) => matches_value(nv.value.toString, op)
    case _ => false
  }

  def matches_value(v: String, op: String): Boolean = op match {
    case "eq" => value == v
    case "lt" => value < v
    case "lte" => value <= v
    case "gt" => value > v
    case "gte" => value >= v
    case _ => false
  }
}

class FunctionValue(val name: String, val args: Seq[Value]) extends Value {
  def matches(v: Value, op: String): Boolean = false
}

class EmptyValue extends Value {
  def matches(v: Value, op: String): Boolean = false
}
