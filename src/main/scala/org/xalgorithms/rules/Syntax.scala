package org.xalgorithms.rules

import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.io.Source

class Step {
}

class PackagedTableReference(val package_name: String, val id: String, val version: String, val name: String) {
}

class Column(val table: Reference, val sources: Seq[TableSource]) {
}

class TableSource(val whens: Seq[When]) {
}

class ColumnTableSource(val name: String, val source: String, whens: Seq[When]) extends TableSource(whens) {
}

class ColumnsTableSource(val columns: Seq[String], whens: Seq[When]) extends TableSource(whens) {
}

class Value {
}

class Reference(val section: String, val key: String) extends Value {
}

class Number(val value: Double) extends Value {
}

class StringValue(val value: String) extends Value {
}

class When(val left: Value, val right: Value, val op: String) {
}

class Assignment(val target: String, val source: Value) {
}

class Assemble(val name: String, val columns: Seq[Column]) extends Step {
}

class Filter(val table: Reference, val filters: Seq[When]) extends Step {
}

class Keep(val name: String, val table: String) extends Step {
}

class AssignmentStep(val table: Reference, val assignments: Seq[Assignment]) extends Step {
}

class MapStep(table: Reference, assignments: Seq[Assignment]) extends AssignmentStep(table, assignments) {
}

class Reduce(
  val filters: Seq[When],
  table: Reference, assignments: Seq[Assignment]) extends AssignmentStep(table, assignments) {
}

class Require(val table_reference: PackagedTableReference, val indexes: Seq[String]) extends Step {
}

class Revise extends Step {
}

object StepProduce {
  implicit val columnReads: Reads[Column] = (
    (JsPath \ "table").read[JsObject] and
    (JsPath \ "sources").read[JsArray]
  )(produce_column _)

  implicit val referenceReads: Reads[Reference] = (
    (JsPath \ "section").read[String] and
    (JsPath \ "key").read[String]
  )(produce_reference _)

  implicit val colsSourceReads: Reads[ColumnsTableSource] = (
    (JsPath \ "columns").read[JsArray] and
    (JsPath \ "whens").read[JsArray]
  )(produce_columns_table_source _)

  implicit val colSourceReads: Reads[ColumnTableSource] = (
    (JsPath \ "name").read[String] and
    (JsPath \ "source").read[String] and
    (JsPath \ "whens").read[JsArray]
  )(produce_column_table_source _)

  implicit val whenReads: Reads[When] = (
    (JsPath \ "left").read[JsObject] and
    (JsPath \ "right").read[JsObject] and
    (JsPath \ "op").read[String]
  )(produce_when _)

  implicit val valueReads: Reads[Value] = (
    (JsPath \ "type").read[String] and
    (JsPath).read[JsObject]
  )(produce_value _)

  implicit val assignmentReads: Reads[Assignment] = (
    (JsPath \ "target").read[String] and
    (JsPath \ "source").read[JsObject]
  )(produce_assignment _)

  def stringOrNull(content: JsObject, k: String): String = {
    return (content \ k).validate[String].getOrElse(null)
  }

  def doubleOrNull(content: JsObject, k: String): Double = {
    var rv = null.asInstanceOf[Double]
    val sv = stringOrNull(content, k)

    if (null != sv) {
      rv = sv.toDouble
    }

    return rv
  }

  def produce_packaged_table_reference(content: JsObject): PackagedTableReference = {
    return new PackagedTableReference(
      stringOrNull(content, "package"),
      stringOrNull(content, "id"),
      stringOrNull(content, "version"),
      stringOrNull(content, "name")
    )
  }

  def produce_columns_table_source(
    columns: JsArray, whens: JsArray): ColumnsTableSource = {
    return new ColumnsTableSource(
      columns.validate[Seq[String]].getOrElse(Seq()),
      whens.validate[Seq[When]].getOrElse(Seq())
    )
  }

  def produce_column_table_source(
    name: String, source: String, whens: JsArray): ColumnTableSource = {
    return new ColumnTableSource(
      name, source,
      whens.validate[Seq[When]].getOrElse(Seq())
    )
  }

  def produce_when(left: JsObject, right: JsObject, op: String): When = {
    return new When(
      left.validate[Value].getOrElse(null),
      right.validate[Value].getOrElse(null),
      op)
  }

  def produce_value(vt: String, content: JsObject): Value = {
    if ("string" == vt) {
      return new StringValue(stringOrNull(content, "value"))
    } else if ("number" == vt) {
      return new Number(doubleOrNull(content, "value"))
    } else if ("reference" == vt) {
      return new Reference(stringOrNull(content, "section"), stringOrNull(content, "key"))
    }

    return null
  }

  def produce_assignment(target: String, source: JsObject): Assignment = {
    return new Assignment(
      target, source.validate[Value].getOrElse(null)
    )
  }

  def produce_column(table_reference: JsObject, sources: JsArray): Column = {
    return new Column(
      table_reference.validate[Reference].getOrElse(null),
      sources.validate[Seq[ColumnsTableSource]].getOrElse(Seq()) ++
        sources.validate[Seq[ColumnTableSource]].getOrElse(Seq())
    )
  }

  def produce_reference(section: String, key: String): Reference = {
    return new Reference(section, key)
  }

  def produce_assemble(content: JsObject): Step = {
    return new Assemble(
      stringOrNull(content, "table_name"),
      (content \ "columns").validate[Seq[Column]].getOrElse(null)
    )
  }

  def produce_filter(content: JsObject): Step = {
    return new Filter(
      (content \ "table").validate[Reference].getOrElse(null),
      (content \ "filters").validate[Seq[When]].getOrElse(Seq())
    )
  }

  def produce_keep(content: JsObject): Step = {
    return new Keep(stringOrNull(content, "name"), stringOrNull(content, "table_name"))
  }

  def produce_map(content: JsObject): Step = {
    return new MapStep(
      (content \ "table").validate[Reference].getOrElse(null),
      (content \ "assignments").validate[Seq[Assignment]].getOrElse(Seq())
    )
  }

  def produce_reduce(content: JsObject): Step = {
    return new Reduce(
      (content \ "filters").validate[Seq[When]].getOrElse(Seq()),
      (content \ "table").validate[Reference].getOrElse(null),
      (content \ "assignments").validate[Seq[Assignment]].getOrElse(Seq())
    )
  }

  def produce_require(content: JsObject): Step = {
    return new Require(
      produce_packaged_table_reference(
        (content \ "reference").as[JsObject]),
      (content \ "indexes").as[Seq[String]])
  }

  def produce_revise(content: JsObject): Step = {
    return new Revise()
  }

  val fns = Map[String, (JsObject) => Step](
    "assemble" -> produce_assemble,
    "filter" -> produce_filter,
    "keep" -> produce_keep,
    "map" -> produce_map,
    "reduce" -> produce_reduce,
    "require" -> produce_require,
    "revise" -> produce_revise
  )

  def apply(name: String, content: JsObject): Step = {
    return fns(name)(content)
  }
}

object SyntaxFromSource {
  implicit val stepReads: Reads[Step] = (
    (JsPath \ "name").read[String] and
    (JsPath).read[JsObject]
  )(StepProduce.apply _)

  def apply(source: Source): Seq[Step] = {
    val res = (Json.parse(source.mkString) \ "steps").validate[Seq[Step]]
    res.getOrElse(Seq())
  }
}
