package org.xalgorithms.rules

import org.xalgorithms.rules.elements.{ PackagedTableReference, Value }

class Context(load: LoadTableSource) {
  var _tables: scala.collection.mutable.Map[String, Seq[Map[String, Value]]] = scala.collection.mutable.Map()
  var _revisions: scala.collection.mutable.Map[String, scala.collection.mutable.Seq[Revision]] = scala.collection.mutable.Map()

  def load(ptref: PackagedTableReference) {
    _tables(ptref.name) = load.load(ptref)
  }

  def retain(section: String, name: String, tbl: Seq[Map[String, Value]]) {
    if ("table" == section) {
      _tables(name) = tbl
    }
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
