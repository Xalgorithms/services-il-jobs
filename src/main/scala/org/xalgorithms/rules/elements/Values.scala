package org.xalgorithms.rules.elements

abstract class Value {
  def matches(v: Value, op: String): Boolean
}

class Reference(val section: String, val key: String) extends Value {
  def matches(v: Value, op: String): Boolean = {
    return false
  }
}

class NumberValue(val value: BigDecimal) extends Value {
  def matches(v: Value, op: String): Boolean = v match {
    case (sv: StringValue) => value == BigDecimal(sv.value)
    case (nv: NumberValue) => value == nv.value
    case _ => false
  }
}

class StringValue(val value: String) extends Value {
  def matches(v: Value, op: String): Boolean = v match {
    case (sv: StringValue) => value == sv.value
    case (nv: NumberValue) => value == nv.toString
    case _ => false
  }
}

class FunctionValue(val name: String, val args: Seq[Value]) extends Value {
  def matches(v: Value, op: String): Boolean = {
    return false
  }
}

class EmptyValue extends Value {
  def matches(v: Value, op: String): Boolean = false
}
