package org.xalgorithms.rules

import org.xalgorithms.rules._

import scala.io.Source
import org.scalatest._

class SyntaxSpec extends FlatSpec with Matchers {
  "Syntax" should "load Assemble from JSON" in {
    val source = Source.fromURL(getClass.getResource("/assemble.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [Assemble]

    val o = steps.head.asInstanceOf[Assemble]
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

  it should "load Filter from JSON" in {
    val source = Source.fromURL(getClass.getResource("/filter.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [Filter]

    val o = steps.head.asInstanceOf[Filter]
  }

  it should "load Keep from JSON" in {
    val source = Source.fromURL(getClass.getResource("/keep.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [Keep]

    val o = steps.head.asInstanceOf[Keep]
    o.name shouldEqual("keep")
    o.table shouldEqual("table0")
  }

  it should "load MapStep from JSON" in {
    val source = Source.fromURL(getClass.getResource("/map.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [MapStep]

    val o = steps.head.asInstanceOf[MapStep]
  }

  it should "load Reduce from JSON" in {
    val source = Source.fromURL(getClass.getResource("/reduce.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [Reduce]

    val o = steps.head.asInstanceOf[Reduce]
  }

  it should "load Require from JSON" in {
    val source = Source.fromURL(getClass.getResource("/require.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [Require]

    val o = steps.head.asInstanceOf[Require]
    o.table_reference should not be null
    o.table_reference.package_name shouldEqual "package"
    o.table_reference.id shouldEqual "id"
    o.table_reference.version shouldEqual "1.2.34"
    o.table_reference.name shouldEqual "table_name"
    o.indexes shouldEqual Seq("a", "b")
  }

  it should "load Revise from JSON" in {
    val source = Source.fromURL(getClass.getResource("/revise.json"))
    val steps = SyntaxFromSource(source)
    steps.length shouldBe 1
    steps.head should not be null
    steps.head shouldBe a [Revise]

    val o = steps.head.asInstanceOf[Revise]
  }
}
