package org.xalgorithms.rules

import org.xalgorithms.rules.elements.{ PackagedTableReference, Value }
import scala.collection.mutable

class Context(load: LoadTableSource) {
  var _tables = mutable.Map[String, Seq[Map[String, Value]]]()
  var _new_tables = mutable.Map[String, mutable.Map[String, Seq[Map[String, Value]]]]()
  var _revisions = mutable.Map[String, mutable.Seq[Revision]]()
  var _maps = mutable.Map[String, Map[String, Value]]()

  def load(ptref: PackagedTableReference) {
    _tables(ptref.name) = load.load(ptref)
  }

  def retain(section: String, name: String, tbl: Seq[Map[String, Value]]) {
    if ("table" == section) {
      _tables(name) = tbl
    }
  }

  def retain_map(section: String, m: Map[String, Value]) {
    _maps(section) = m
  }

  def retain_table(section: String, key: String, t: Seq[Map[String, Value]]) {
    var sm = _new_tables.getOrElse(section, mutable.Map[String, Seq[Map[String, Value]]]())
    sm.put(key, t)
    _new_tables(section) = sm
  }

  def lookup_in_map(section: String, key: String): Value = {
    _maps.getOrElse(section, Map[String, Value]()).getOrElse(key, null)
  }

  def lookup_table(section: String, table_name: String): Seq[Map[String, Value]] = {
    _new_tables.getOrElse(section, mutable.Map[String, Seq[Map[String, Value]]]()).getOrElse(table_name, null)
  }

  def find_in_section(name: String, key: String): Seq[Map[String, Value]] = {
    if ("table" == name) {
      return _tables.getOrElse(key, null)
    }

    return Seq()
  }

  def revisions(): Map[String, Seq[Revision]] = {
    return _revisions.toMap
  }

  def add_revision(key: String, rev: Revision) {
    val current = _revisions.getOrElse(key, scala.collection.mutable.Seq())
    _revisions.put(key, current ++ scala.collection.mutable.Seq(rev))
  }
}
