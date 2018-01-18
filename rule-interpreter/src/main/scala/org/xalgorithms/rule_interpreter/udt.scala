package org.xalgorithms.rule_interpreter

import play.api.libs.json.Json

object udt {
  case class Table(`type`: String, section: String, key: String)
  case class Assignment(column: Option[String], `type`: String, name: Option[String], section: Option[String], key: Option[String], value: Option[String], args: Option[List[Assignment]])
  case class Step(name: String, table: Table, assignments: List[Assignment])

  implicit val tableReads = Json.reads[Table]
  implicit val assignmentReads = Json.reads[Assignment]
  implicit val stepReads = Json.reads[Step]
}