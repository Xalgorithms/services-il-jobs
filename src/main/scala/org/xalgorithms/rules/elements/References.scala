package org.xalgorithms.rules.elements

import org.xalgorithms.rules.{ Context }

class Reference(val section: String, val key: String) extends Value {
  def matches(v: Value, op: String): Boolean = op match {
    case "eq" => matches_reference(v)
    case _ => false
  }

  def matches_reference(v: Value): Boolean = v match {
    case (rv: Reference) => section == rv.section && key == rv.key
    case _ => false
  }
}

class TableReference(section: String, table_name: String) extends Reference(section, table_name) {
  def get(ctx: Context): Seq[Map[String, Value]] = {
    ctx.lookup_table(section, table_name)
  }

  override def matches(v: Value, op: String): Boolean = v match {
    case (trv: TableReference) => super.matches(v, op)
    case _ => false
  }
}

abstract class ValueReference(section: String, key: String) extends Reference(section, key) {
  def get(ctx: Context): Value
}

class DocumentValueReference(section: String, key: String) extends ValueReference(section, key) {
  def get(ctx: Context): Value = {
    ctx.lookup_in_map(section, key)
  }

  override def matches(v: Value, op: String): Boolean = v match {
    case (mrv: DocumentValueReference) => super.matches(v, op)
    case _ => false
  }
}

object MakeReference {
  def apply(section: String, key: String): Reference = section match {
    case "envelope" => new DocumentValueReference(section, key)
    case "_local" => new DocumentValueReference(section, key)
    case "_context" => new DocumentValueReference(section, key)
    case _ => new TableReference(section, key)
  }
}
