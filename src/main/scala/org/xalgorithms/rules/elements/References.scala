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

class TableReference(section: String, key: String) extends Reference(section, key) {
  def get(ctx: Context): Seq[Map[String, Value]] = {
    ctx.lookup_table_in_section(section, key)
  }

  override def matches(v: Value, op: String): Boolean = v match {
    case (trv: TableReference) => super.matches(v, op)
    case _ => false
  }
}

class MapReference(section: String, key: String) extends Reference(section, key) {
  def get(ctx: Context): Map[String, Value] = {
    ctx.lookup_map_in_section(section, key)
  }

  override def matches(v: Value, op: String): Boolean = v match {
    case (mrv: MapReference) => super.matches(v, op)
    case _ => false
  }
}

class ReferenceContext(val reference: Reference) {
}

class TableReferenceContext(reference: Reference) extends ReferenceContext(reference) {
}

class ScalarReferenceContext(reference: Reference) extends ReferenceContext(reference) {
}

object MakeReference {
  def apply(section: String, key: String): Reference = section match {
    case "envelope" => new MapReference(section, key)
    case _ => new TableReference(section, key)
  }
}
