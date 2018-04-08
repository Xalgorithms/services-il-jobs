package org.xalgorithms.rules.steps

import org.scalatest._
import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._
import org.xalgorithms.rules.steps._

class AssembleStepSpec extends FlatSpec with Matchers {
  "AssembleStep" should "load tables into the Context" in {
    val ctx = new Context(new ResourceLoadTableSource())

    // two simple COLUMNS
    // ALL columns, no conditions
    val cols0 = new Column(
      new Reference("table", "table0"),
      Seq(new ColumnsTableSource(Seq(), Seq()))
    )
    // only (aa, bb), no conditions
    val cols1 = new Column(
      new Reference("table", "table1"),
      Seq(new ColumnsTableSource(Seq("aa", "bb"), Seq()))
    )

    val step = new AssembleStep("table_assemble", Seq(cols0, cols1))

    ctx.load(new PackagedTableReference("package", "table0", "0.0.1", "table0"))
    ctx.load(new PackagedTableReference("package", "table1", "0.0.1", "table1"))

    step.execute(ctx)

    val tbl = ctx.find_in_section("table", "table_assemble")
    tbl should not be null
    tbl.length shouldEqual(3)
    tbl(0)("a") shouldBe a [NumberValue]
    tbl(0)("a").asInstanceOf[NumberValue].value shouldEqual(1.0)
    tbl(0)("b") shouldBe a [StringValue]
    tbl(0)("b").asInstanceOf[StringValue].value shouldEqual("foo")
    tbl(0)("aa") shouldBe a [NumberValue]
    tbl(0)("aa").asInstanceOf[NumberValue].value shouldEqual(1.0)
    tbl(0)("aa") shouldBe a [NumberValue]
    tbl(0)("bb").asInstanceOf[NumberValue].value shouldEqual(2.0)
    tbl(0).get("cc") shouldEqual(None)
    tbl(1)("a") shouldBe a [NumberValue]
    tbl(1)("a").asInstanceOf[NumberValue].value shouldEqual(2.0)
    tbl(1)("b") shouldBe a [StringValue]
    tbl(1)("b").asInstanceOf[StringValue].value shouldEqual("bar")
    tbl(1)("aa") shouldBe a [NumberValue]
    tbl(1)("aa").asInstanceOf[NumberValue].value shouldEqual(1.0)
    tbl(1)("aa") shouldBe a [NumberValue]
    tbl(1)("bb").asInstanceOf[NumberValue].value shouldEqual(2.0)
    tbl(1).get("cc") shouldEqual(None)
    tbl(2)("a") shouldBe a [NumberValue]
    tbl(2)("a").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(2)("b") shouldBe a [StringValue]
    tbl(2)("b").asInstanceOf[StringValue].value shouldEqual("baz")
    tbl(2)("aa") shouldBe a [NumberValue]
    tbl(2)("aa").asInstanceOf[NumberValue].value shouldEqual(1.0)
    tbl(2)("aa") shouldBe a [NumberValue]
    tbl(2)("bb").asInstanceOf[NumberValue].value shouldEqual(2.0)
    tbl(2).get("cc") shouldEqual(None)
  }
}
