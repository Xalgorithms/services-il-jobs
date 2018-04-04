package org.xalgorithms.rules

import org.xalgorithms.rules._
import org.xalgorithms.rules.steps._
import org.xalgorithms.rules.elements._

import scala.io.Source
import org.scalatest._

class SyntaxSpec extends FlatSpec with Matchers {
  "AssembleStep" should "load from JSON" in {
    val source = Source.fromURL(getClass.getResource("/assemble.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [AssembleStep]

    val o = steps.head.asInstanceOf[AssembleStep]
    o.name shouldEqual("table_final")

    o.columns.length shouldBe 2

    o.columns(0).table should not be null
    o.columns(0).table.section shouldEqual("tables")
    o.columns(0).table.key shouldEqual("table0")
    o.columns(0).sources.length shouldBe 1
    o.columns(0).sources(0) should not be null
    o.columns(0).sources(0) shouldBe a [ColumnsTableSource]
    o.columns(0).sources(0).asInstanceOf[ColumnsTableSource].columns shouldEqual(Seq("c0", "c1", "c2"))
    o.columns(0).sources(0).whens.length shouldBe 1
    o.columns(0).sources(0).whens(0) should not be null
    o.columns(0).sources(0).whens(0).left should not be null
    o.columns(0).sources(0).whens(0).left shouldBe a [Reference]
    o.columns(0).sources(0).whens(0).left.asInstanceOf[Reference].section shouldEqual("_context")
    o.columns(0).sources(0).whens(0).left.asInstanceOf[Reference].key shouldEqual("a")
    o.columns(0).sources(0).whens(0).right should not be null
    o.columns(0).sources(0).whens(0).right shouldBe a [StringValue]
    o.columns(0).sources(0).whens(0).right.asInstanceOf[StringValue].value shouldEqual("a distant ship")
    o.columns(0).sources(0).whens(0).op shouldEqual("eq")

    o.columns(1).table should not be null
    o.columns(1).table.section shouldEqual("tables")
    o.columns(1).table.key shouldEqual("table1")
    o.columns(1).sources.length shouldBe 1
    o.columns(1).sources(0) should not be null
    o.columns(1).sources(0) shouldBe a [ColumnTableSource]
    o.columns(1).sources(0).asInstanceOf[ColumnTableSource].name shouldEqual("y")
    o.columns(1).sources(0).asInstanceOf[ColumnTableSource].source shouldEqual("x")
    o.columns(1).sources(0).whens.length shouldBe 1
    o.columns(1).sources(0).whens(0) should not be null
    o.columns(1).sources(0).whens(0).left should not be null
    o.columns(1).sources(0).whens(0).left shouldBe a [Reference]
    o.columns(1).sources(0).whens(0).left.asInstanceOf[Reference].section shouldEqual("_local")
    o.columns(1).sources(0).whens(0).left.asInstanceOf[Reference].key shouldEqual("x")
    o.columns(1).sources(0).whens(0).right should not be null
    o.columns(1).sources(0).whens(0).right shouldBe a [Number]
    o.columns(1).sources(0).whens(0).right.asInstanceOf[Number].value shouldEqual(1.0)
    o.columns(1).sources(0).whens(0).op shouldEqual("eq")
  }

  "FilterStep" should "load from JSON" in {
    val source = Source.fromURL(getClass.getResource("/filter.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [FilterStep]

    val o = steps.head.asInstanceOf[FilterStep]

    o.table should not be null
    o.table.section shouldEqual("tables")
    o.table.key shouldEqual("table0")

    o.filters.length shouldBe 1
    o.filters(0) should not be null
    o.filters(0).left should not be null
    o.filters(0).left shouldBe a [Reference]
    o.filters(0).left.asInstanceOf[Reference].section shouldEqual("_context")
    o.filters(0).left.asInstanceOf[Reference].key shouldEqual("a")
    o.filters(0).right should not be null
    o.filters(0).right shouldBe a [Number]
    o.filters(0).right.asInstanceOf[Number].value shouldEqual(3.0)
    o.filters(0).op shouldEqual("lt")
  }

  "KeepStep" should "load from JSON" in {
    val source = Source.fromURL(getClass.getResource("/keep.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [KeepStep]

    val o = steps.head.asInstanceOf[KeepStep]
    o.name shouldEqual("keep")
    o.table shouldEqual("table0")
  }

  "MapStep" should "load from JSON" in {
    val source = Source.fromURL(getClass.getResource("/map.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [MapStep]

    val o = steps.head.asInstanceOf[MapStep]

    o.table should not be null
    o.table.section shouldEqual("tables")
    o.table.key shouldEqual("items")

    o.assignments.length shouldBe 3

    o.assignments(0) should not be null
    o.assignments(0).target shouldEqual("a.b.c")
    o.assignments(0).source should not be null
    o.assignments(0).source shouldBe a [Reference]
    o.assignments(0).source.asInstanceOf[Reference].section shouldEqual("_context")
    o.assignments(0).source.asInstanceOf[Reference].key shouldEqual("x.y.z")

    o.assignments(1) should not be null
    o.assignments(1).target shouldEqual("c")
    o.assignments(1).source should not be null
    o.assignments(1).source shouldBe a [Number]
    o.assignments(1).source.asInstanceOf[Number].value shouldEqual(2.0)

    o.assignments(2) should not be null
    o.assignments(2).target shouldEqual("d")
    o.assignments(2).source should not be null
    o.assignments(2).source shouldBe a [StringValue]
    o.assignments(2).source.asInstanceOf[StringValue].value shouldEqual("s")
  }

  it should "read function usings from JSON" in {
    val source = Source.fromURL(getClass.getResource("/map_functions.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [MapStep]

    val o = steps.head.asInstanceOf[MapStep]

    o.assignments.length shouldBe 1

    o.assignments(0).source should not be null
    o.assignments(0).source shouldBe a [FunctionValue]
    o.assignments(0).source.asInstanceOf[FunctionValue].name shouldEqual("multiply")
    o.assignments(0).source.asInstanceOf[FunctionValue].args.length shouldEqual(2)
    o.assignments(0).source.asInstanceOf[FunctionValue].args(0) shouldBe a [FunctionValue]

    val arg0 = o.assignments(0).source.asInstanceOf[FunctionValue].args(0).asInstanceOf[FunctionValue]
    arg0.name shouldEqual("add")
    arg0.args.length shouldEqual(2)
    arg0.args(0) shouldBe a [Reference]
    arg0.args(0).asInstanceOf[Reference].section shouldEqual("_context")
    arg0.args(0).asInstanceOf[Reference].key shouldEqual("b")
    arg0.args(1) shouldBe a [Number]
    arg0.args(1).asInstanceOf[Number].value shouldEqual(2.0)

    o.assignments(0).source.asInstanceOf[FunctionValue].args(1) shouldBe a [Number]
    o.assignments(0).source.asInstanceOf[FunctionValue].args(1).asInstanceOf[Number].value shouldEqual(4.0)
  }

  "ReduceStep" should "load from JSON" in {
    val source = Source.fromURL(getClass.getResource("/reduce.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [ReduceStep]

    val o = steps.head.asInstanceOf[ReduceStep]

    o.table should not be null
    o.table.section shouldEqual("tables")
    o.table.key shouldEqual("foo")

    o.assignments.length shouldBe 1
    o.assignments(0) should not be null
    o.assignments(0).target shouldEqual("a")
    o.assignments(0).source should not be null
    o.assignments(0).source shouldBe a [Reference]
    o.assignments(0).source.asInstanceOf[Reference].section shouldEqual("_context")
    o.assignments(0).source.asInstanceOf[Reference].key shouldEqual("b")

    o.filters.length shouldBe 1
    o.filters(0) should not be null
    o.filters(0).left should not be null
    o.filters(0).left shouldBe a [Reference]
    o.filters(0).left.asInstanceOf[Reference].section shouldEqual("_context")
    o.filters(0).left.asInstanceOf[Reference].key shouldEqual("c")
    o.filters(0).right should not be null
    o.filters(0).right shouldBe a [Reference]
    o.filters(0).right.asInstanceOf[Reference].section shouldEqual("_context")
    o.filters(0).right.asInstanceOf[Reference].key shouldEqual("a")
    o.filters(0).op shouldEqual("eq")
  }

  it should "read function usings from JSON" in {
    val source = Source.fromURL(getClass.getResource("/reduce_functions.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [ReduceStep]

    val o = steps.head.asInstanceOf[ReduceStep]
    o.assignments.length shouldBe 1

    o.assignments(0).source should not be null
    o.assignments(0).source shouldBe a [FunctionValue]
    o.assignments(0).source.asInstanceOf[FunctionValue].name shouldEqual("add")
    o.assignments(0).source.asInstanceOf[FunctionValue].args.length shouldEqual(2)
    o.assignments(0).source.asInstanceOf[FunctionValue].args(0) shouldBe a [FunctionValue]

    val arg0 = o.assignments(0).source.asInstanceOf[FunctionValue].args(0).asInstanceOf[FunctionValue]
    arg0.name shouldEqual("multiply")
    arg0.args.length shouldEqual(2)
    arg0.args(0) shouldBe a [Reference]
    arg0.args(0).asInstanceOf[Reference].section shouldEqual("_context")
    arg0.args(0).asInstanceOf[Reference].key shouldEqual("b")
    arg0.args(1) shouldBe a [Reference]
    arg0.args(1).asInstanceOf[Reference].section shouldEqual("_context")
    arg0.args(1).asInstanceOf[Reference].key shouldEqual("c")

    o.assignments(0).source.asInstanceOf[FunctionValue].args(1) shouldBe a [Reference]
    o.assignments(0).source.asInstanceOf[FunctionValue].args(1).asInstanceOf[Reference].section shouldEqual("_context")
    o.assignments(0).source.asInstanceOf[FunctionValue].args(1).asInstanceOf[Reference].key shouldEqual("d")
  }

  "RequireStep" should "load from JSON" in {
    val source = Source.fromURL(getClass.getResource("/require.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [RequireStep]

    val o = steps.head.asInstanceOf[RequireStep]
    o.table_reference should not be null
    o.table_reference.package_name shouldEqual "package"
    o.table_reference.id shouldEqual "id"
    o.table_reference.version shouldEqual "1.2.34"
    o.table_reference.name shouldEqual "table_name"
    o.indexes shouldEqual Seq("a", "b")
  }

  "ReviseStep" should "load from JSON" in {
    val source = Source.fromURL(getClass.getResource("/revise.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [ReviseStep]

    val o = steps.head.asInstanceOf[ReviseStep]

    o.table should not be null
    o.table.section shouldEqual("tables")
    o.table.key shouldEqual("items")

    o.revisions.length shouldEqual(3)
    o.revisions(0) should not be null
    o.revisions(0).source should not be null
    o.revisions(0).source shouldBe a [AddRevisionSource]
    o.revisions(0).source.column shouldEqual("a.b")
    o.revisions(0).source.asInstanceOf[TableRevisionSource].table.section shouldEqual("table")
    o.revisions(0).source.asInstanceOf[TableRevisionSource].table.key shouldEqual("foo")
    o.revisions(0).source.whens.length shouldEqual(1)
    o.revisions(0).source.whens(0) should not be null
    o.revisions(0).source.whens(0).left shouldBe a [Reference]
    o.revisions(0).source.whens(0).left.asInstanceOf[Reference].section shouldEqual("_local")
    o.revisions(0).source.whens(0).left.asInstanceOf[Reference].key shouldEqual("x")
    o.revisions(0).source.whens(0).right shouldBe a [Reference]
    o.revisions(0).source.whens(0).right.asInstanceOf[Reference].section shouldEqual("_context")
    o.revisions(0).source.whens(0).right.asInstanceOf[Reference].key shouldEqual("y")
    o.revisions(0).source.whens(0).op shouldEqual("eq")

    o.revisions(1) should not be null
    o.revisions(1).source should not be null
    o.revisions(1).source shouldBe a [UpdateRevisionSource]
    o.revisions(1).source.column shouldEqual("c")
    o.revisions(1).source.asInstanceOf[TableRevisionSource].table.section shouldEqual("table")
    o.revisions(1).source.asInstanceOf[TableRevisionSource].table.key shouldEqual("bar")
    o.revisions(1).source.whens.length shouldEqual(1)
    o.revisions(1).source.whens(0) should not be null
    o.revisions(1).source.whens(0).left shouldBe a [Reference]
    o.revisions(1).source.whens(0).left.asInstanceOf[Reference].section shouldEqual("_context")
    o.revisions(1).source.whens(0).left.asInstanceOf[Reference].key shouldEqual("q")
    o.revisions(1).source.whens(0).right shouldBe a [Number]
    o.revisions(1).source.whens(0).right.asInstanceOf[Number].value shouldEqual(3.0)
    o.revisions(1).source.whens(0).op shouldEqual("lt")

    o.revisions(2) should not be null
    o.revisions(2).source should not be null
    o.revisions(2).source shouldBe a [DeleteRevisionSource]
    o.revisions(2).source.column shouldEqual("d")
    o.revisions(2).source.whens.length shouldEqual(1)
    o.revisions(2).source.whens(0) should not be null
    o.revisions(2).source.whens(0).left shouldBe a [Reference]
    o.revisions(2).source.whens(0).left.asInstanceOf[Reference].section shouldEqual("_context")
    o.revisions(2).source.whens(0).left.asInstanceOf[Reference].key shouldEqual("r")
    o.revisions(2).source.whens(0).right shouldBe a [Number]
    o.revisions(2).source.whens(0).right.asInstanceOf[Number].value shouldEqual(1.0)
    o.revisions(2).source.whens(0).op shouldEqual("eq")
  }
}
