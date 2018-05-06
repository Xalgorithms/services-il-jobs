package org.xalgorithms.rules

import org.xalgorithms.rules.elements.{ PackagedTableReference, Value }
import scala.collection.mutable

abstract class Context {
  def load(ptref: PackagedTableReference)
  def retain_map(section: String, m: Map[String, Value])
  def retain_table(section: String, key: String, t: Seq[Map[String, Value]])
  def lookup_in_map(section: String, key: String): Value
  def lookup_table(section: String, table_name: String): Seq[Map[String, Value]]
  def revisions(): Map[String, Seq[Revision]]
  def add_revision(key: String, rev: Revision)
}

class GlobalContext(load: LoadTableSource) extends Context {
  var _tables = mutable.Map[String, mutable.Map[String, Seq[Map[String, Value]]]]()
  var _revisions = mutable.Map[String, mutable.Seq[Revision]]()
  var _maps = mutable.Map[String, Map[String, Value]]()

  def load(ptref: PackagedTableReference) {
    retain_table("table", ptref.name, load.load(ptref))
  }

  def retain_map(section: String, m: Map[String, Value]) {
    _maps(section) = m
  }

  def retain_table(section: String, key: String, t: Seq[Map[String, Value]]) {
    val sm = _tables.getOrElse(section, mutable.Map[String, Seq[Map[String, Value]]]())
    sm.put(key, t)
    _tables(section) = sm
  }

  def lookup_in_map(section: String, key: String): Value = {
    _maps.getOrElse(section, Map[String, Value]()).getOrElse(key, null)
  }

  def lookup_table(section: String, table_name: String): Seq[Map[String, Value]] = {
    _tables.getOrElse(section, mutable.Map[String, Seq[Map[String, Value]]]()).getOrElse(table_name, null)
  }

  def revisions(): Map[String, Seq[Revision]] = {
    return _revisions.toMap
  }

  def add_revision(key: String, rev: Revision) {
    val current = _revisions.getOrElse(key, scala.collection.mutable.Seq())
    _revisions.put(key, current ++ scala.collection.mutable.Seq(rev))
  }
}

class RowContext(ctx: Context, local_row: Map[String, Value], context_row: Map[String, Value]) extends Context {
  def load(ptref: PackagedTableReference) = ctx.load(ptref)
  def retain_map(section: String, m: Map[String, Value]) = ctx.retain_map(section, m)
  def retain_table(section: String, key: String, t: Seq[Map[String, Value]]) = ctx.retain_table(section, key, t)

  def lookup_in_map(section: String, key: String): Value = {
    if ("_local" == section) {
      return local_row.getOrElse(key, null)
    } else if ("_context" == section) {
      return context_row.getOrElse(key, null)
    }

    return ctx.lookup_in_map(section, key)
  }

  def lookup_table(section: String, table_name: String): Seq[Map[String, Value]] = ctx.lookup_table(section, table_name)

  def revisions(): Map[String, Seq[Revision]] = ctx.revisions()

  def add_revision(key: String, rev: Revision) = ctx.add_revision(key, rev)
}
