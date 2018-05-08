package org.xalgorithms.rules.elements

import org.xalgorithms.rules.{ Context }

abstract class Value {
  def matches(v: Value, op: String): Boolean
}

abstract class IntrinsicValue extends Value {
  def apply_func(args: Seq[Value], func: String): Option[Value]
}

class NumberValue(val value: BigDecimal) extends IntrinsicValue {
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

  def apply_func(args: Seq[Value], func: String): Option[Value] = func match {
    case "add" => Some(sum(args))
    case _ => None
  }

  def sum(args: Seq[Value]): Value = {
    new NumberValue(args.foldLeft(value) { (sum, v) =>
      v match {
        case (nv: NumberValue) => sum + nv.value
        case (sv: StringValue) => {
          try {
            sum + BigDecimal(sv.value)
          } catch {
            case _: Throwable => sum
          }
        }
        case _ => sum
      }
    })
  }
}

class StringValue(val value: String) extends IntrinsicValue {
  def matches(v: Value, op: String): Boolean = v match {
    case (sv: StringValue) => matches_value(sv.value, op)
    case (nv: NumberValue) => matches_value(nv.value.toString, op)
    case _ => false
  }

  def apply_func(args: Seq[Value], func: String): Option[Value] = func match {
    case "add" => Some(concat(args))
    case _ => None
  }

  def matches_value(v: String, op: String): Boolean = op match {
    case "eq" => value == v
    case "lt" => value < v
    case "lte" => value <= v
    case "gt" => value > v
    case "gte" => value >= v
    case _ => false
  }

  def concat(args: Seq[Value]): Value = {
    new StringValue(args.foldLeft(value) { (s, v) =>
      v match {
        case (sv: StringValue) => s + sv.value
        case (nv: NumberValue) => s + nv.value.toString
        case _ => s
      }
    })
  }
}

class FunctionValue(val name: String, val args: Seq[Value]) extends Value {
  def matches(v: Value, op: String): Boolean = false
}

class EmptyValue extends Value {
  def matches(v: Value, op: String): Boolean = false
}

abstract class ReferenceValue(val section: String, val key: String) extends Value {
  def resolve(ctx: Context): Value

  def matches(v: Value, op: String): Boolean = v match {
    case (vr: ReferenceValue) => if ("eq" == op) section == vr.section && key == vr.key else false
    case _ => false
  }
}

class DocumentReferenceValue(section: String, key: String) extends ReferenceValue(section, key) {
  def resolve(ctx: Context): Value = {
    ctx.lookup_in_map(section, key)
  }
}

object ResolveValue {
  def apply(v: Value, ctx: Context): Value = v match {
    case (rv: ReferenceValue) => rv.resolve(ctx)
    case _ => v
  }
}
