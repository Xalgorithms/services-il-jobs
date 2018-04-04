package org.xalgorithms.rules.elements

class PackagedTableReference(val package_name: String, val id: String, val version: String, val name: String) {
}

class Column(val table: Reference, val sources: Seq[TableSource]) {
}

class When(val left: Value, val right: Value, val op: String) {
}

class Assignment(val target: String, val source: Value) {
}

class Revision(val source: RevisionSource) {
}
