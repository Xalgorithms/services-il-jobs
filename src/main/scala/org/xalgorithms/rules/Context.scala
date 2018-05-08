package org.xalgorithms.rules

import org.xalgorithms.rules.elements.{ PackagedTableReference, IntrinsicValue }
import scala.collection.mutable

abstract class Context {
  def load(ptref: PackagedTableReference)
  def retain_map(section: String, m: Map[String, IntrinsicValue])
  def retain_table(section: String, key: String, t: Seq[Map[String, IntrinsicValue]])
  def lookup_in_map(section: String, key: String): IntrinsicValue
  def lookup_table(section: String, table_name: String): Seq[Map[String, IntrinsicValue]]
  def revisions(): Map[String, Seq[Revision]]
  def add_revision(key: String, rev: Revision)
}

class GlobalContext(load: LoadTableSource) extends Context {
  var _tables = mutable.Map[String, mutable.Map[String, Seq[Map[String, IntrinsicValue]]]]()
  var _revisions = mutable.Map[String, mutable.Seq[Revision]]()
  var _maps = mutable.Map[String, Map[String, IntrinsicValue]]()

  def load(ptref: PackagedTableReference) {
    retain_table("table", ptref.name, load.load(ptref))
  }

  def retain_map(section: String, m: Map[String, IntrinsicValue]) {
    _maps(section) = m
  }

  def retain_table(section: String, key: String, t: Seq[Map[String, IntrinsicValue]]) {
    val sm = _tables.getOrElse(section, mutable.Map[String, Seq[Map[String, IntrinsicValue]]]())
    sm.put(key, t)
    _tables(section) = sm
  }

  def lookup_in_map(section: String, key: String): IntrinsicValue = {
    _maps.getOrElse(section, Map[String, IntrinsicValue]()).getOrElse(key, null)
  }

  def lookup_table(section: String, table_name: String): Seq[Map[String, IntrinsicValue]] = {
    _tables.getOrElse(section, mutable.Map[String, Seq[Map[String, IntrinsicValue]]]()).getOrElse(table_name, null)
  }

  def revisions(): Map[String, Seq[Revision]] = {
    return _revisions.toMap
  }

  def add_revision(key: String, rev: Revision) {
    val current = _revisions.getOrElse(key, scala.collection.mutable.Seq())
    _revisions.put(key, current ++ scala.collection.mutable.Seq(rev))
  }
}

class RowContext(ctx: Context, local_row: Map[String, IntrinsicValue], context_row: Map[String, IntrinsicValue]) extends Context {
  def load(ptref: PackagedTableReference) = ctx.load(ptref)
  def retain_map(section: String, m: Map[String, IntrinsicValue]) = ctx.retain_map(section, m)
  def retain_table(section: String, key: String, t: Seq[Map[String, IntrinsicValue]]) = ctx.retain_table(section, key, t)

  def lookup_in_map(section: String, key: String): IntrinsicValue = {
    if ("_local" == section) {
      return local_row.getOrElse(key, null)
    } else if ("_context" == section) {
      return context_row.getOrElse(key, null)
    }

    return ctx.lookup_in_map(section, key)
  }

  def lookup_table(section: String, table_name: String): Seq[Map[String, IntrinsicValue]] = ctx.lookup_table(section, table_name)

  def revisions(): Map[String, Seq[Revision]] = ctx.revisions()

  def add_revision(key: String, rev: Revision) = ctx.add_revision(key, rev)
}
