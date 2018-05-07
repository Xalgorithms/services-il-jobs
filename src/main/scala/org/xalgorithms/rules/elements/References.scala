package org.xalgorithms.rules.elements

import org.xalgorithms.rules.{ Context }

class TableReference(val section: String, val name: String) {
  def get(ctx: Context): Seq[Map[String, Value]] = {
    ctx.lookup_table(section, name)
  }
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
