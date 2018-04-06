package org.xalgorithms.rules

import org.xalgorithms.rules.elements.{ PackagedTableReference, Value }

class Context(load: LoadTableSource) {
  var _tables: scala.collection.mutable.Map[String, Seq[Map[String, Value]]] = scala.collection.mutable.Map()

  def load(ptref: PackagedTableReference) {
    println(ptref)
    println(ptref.name)
    _tables(ptref.name) = load.load(ptref)
  }

  def find_in_section(name: String, key: String): Seq[Map[String, Value]] = {
    if ("table" == name) {
      return _tables(key)
    }

    return Seq()
  }
}
