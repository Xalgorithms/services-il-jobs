package org.xalgorithms.rules.steps

import org.scalatest._
import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._
import org.xalgorithms.rules.steps._

class FilterStepSpec extends FlatSpec with Matchers {
  "FilterStep" should "transform tables in place (number/eq)" in {
    val ctx = new GlobalContext(new ResourceLoadTableSource())
    ctx.load(new PackagedTableReference("package", "table2", "0.0.1", "table2"))

    val step = new FilterStep(
      new TableReference("table", "table2"),
      Seq(
        new When(
          new DocumentReferenceValue("_context", "a"),
          new DocumentReferenceValue("_context", "d"), "eq")))

    step.execute(ctx)

    val tbl = ctx.lookup_table("table", "table2")
    tbl should not be null
    tbl.length shouldEqual(3)

    tbl(0)("a") shouldBe a [NumberValue]
    tbl(0)("a").asInstanceOf[NumberValue].value shouldEqual(2.0)
    tbl(0)("d") shouldBe a [NumberValue]
    tbl(0)("d").asInstanceOf[NumberValue].value shouldEqual(2.0)
    tbl(0)("b") shouldBe a [StringValue]
    tbl(0)("b").asInstanceOf[StringValue].value shouldEqual("bar")

    tbl(1)("a") shouldBe a [NumberValue]
    tbl(1)("a").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(1)("d") shouldBe a [NumberValue]
    tbl(1)("d").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(1)("b") shouldBe a [StringValue]
    tbl(1)("b").asInstanceOf[StringValue].value shouldEqual("baz")

    tbl(2)("a") shouldBe a [NumberValue]
    tbl(2)("a").asInstanceOf[NumberValue].value shouldEqual(4.0)
    tbl(2)("d") shouldBe a [NumberValue]
    tbl(2)("d").asInstanceOf[NumberValue].value shouldEqual(4.0)
    tbl(2)("b") shouldBe a [StringValue]
    tbl(2)("b").asInstanceOf[StringValue].value shouldEqual("fib")
  }

  it should "transform tables in place (string/eq)" in {
    val ctx = new GlobalContext(new ResourceLoadTableSource())
    ctx.load(new PackagedTableReference("package", "table2", "0.0.1", "table2"))

    val step = new FilterStep(
      new TableReference("table", "table2"),
      Seq(
        new When(
          new DocumentReferenceValue("_context", "b"),
          new DocumentReferenceValue("_context", "c"), "eq")))

    step.execute(ctx)

    val tbl = ctx.lookup_table("table", "table2")
    tbl should not be null
    tbl.length shouldEqual(3)

    tbl(0)("a") shouldBe a [NumberValue]
    tbl(0)("a").asInstanceOf[NumberValue].value shouldEqual(1.0)
    tbl(0)("b") shouldBe a [StringValue]
    tbl(0)("b").asInstanceOf[StringValue].value shouldEqual("foo")
    tbl(0)("c") shouldBe a [StringValue]
    tbl(0)("c").asInstanceOf[StringValue].value shouldEqual("foo")

    tbl(1)("a") shouldBe a [NumberValue]
    tbl(1)("a").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(1)("b") shouldBe a [StringValue]
    tbl(1)("b").asInstanceOf[StringValue].value shouldEqual("baz")
    tbl(1)("c") shouldBe a [StringValue]
    tbl(1)("c").asInstanceOf[StringValue].value shouldEqual("baz")

    tbl(2)("a") shouldBe a [NumberValue]
    tbl(2)("a").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(2)("b") shouldBe a [StringValue]
    tbl(2)("b").asInstanceOf[StringValue].value shouldEqual("foo")
    tbl(2)("c") shouldBe a [StringValue]
    tbl(2)("c").asInstanceOf[StringValue].value shouldEqual("foo")
  }
}
