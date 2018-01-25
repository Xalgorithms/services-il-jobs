package org.xalgorithms.rule_interpreter

import play.api.libs.json.Json


object udt {
  case class Table(`type`: String, section: String, key: String)
  case class Assignment(column: Option[String], `type`: String, name: Option[String], section: Option[String], key: Option[String], value: Option[String], args: Option[List[Assignment]])
  case class Expression(left: Assignment, right: Assignment, op: String)
  case class Source(name: String, source: String, expr: Expression)
  case class Column(table: String, sources: List[Source])
  case class Step(name: String, table: Option[Table], assignments: Option[List[Assignment]], table_name: Option[String], columns: Option[List[Column]])

  implicit val tableReads = Json.reads[Table]
  implicit val assignmentReads = Json.reads[Assignment]
  implicit val expressionReads = Json.reads[Expression]
  implicit val sourceReads = Json.reads[Source]
  implicit val columnReads = Json.reads[Column]
  implicit val stepReads = Json.reads[Step]
}