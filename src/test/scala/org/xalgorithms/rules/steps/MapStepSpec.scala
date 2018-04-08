package org.xalgorithms.rules.steps

import org.scalatest._
import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._
import org.xalgorithms.rules.steps._

class MapStepSpec extends FlatSpec with Matchers {
  "MapStep" should "transform tables in place" in {
    val ctx = new Context(new ResourceLoadTableSource())
    ctx.load(new PackagedTableReference("package", "table2", "0.0.1", "table2"))

    val step = new MapStep(
      new Reference("table", "table2"),
      Seq(
        new Assignment("e", new Reference("_context", "a")),
        new Assignment("f", new Reference("_context", "b"))))

    step.execute(ctx)

    val tbl = ctx.find_in_section("table", "table2")
    tbl should not be null
    tbl.length shouldEqual(5)    

    tbl(0)("a") shouldBe a [NumberValue]
    tbl(0)("a").asInstanceOf[NumberValue].value shouldEqual(1.0)
    tbl(0)("e") shouldBe a [NumberValue]
    tbl(0)("e").asInstanceOf[NumberValue].value shouldEqual(1.0)
    tbl(0)("b") shouldBe a [StringValue]
    tbl(0)("b").asInstanceOf[StringValue].value shouldEqual("foo")
    tbl(0)("f") shouldBe a [StringValue]
    tbl(0)("f").asInstanceOf[StringValue].value shouldEqual("foo")

    tbl(1)("a") shouldBe a [NumberValue]
    tbl(1)("a").asInstanceOf[NumberValue].value shouldEqual(2.0)
    tbl(1)("e") shouldBe a [NumberValue]
    tbl(1)("e").asInstanceOf[NumberValue].value shouldEqual(2.0)
    tbl(1)("b") shouldBe a [StringValue]
    tbl(1)("b").asInstanceOf[StringValue].value shouldEqual("bar")
    tbl(1)("f") shouldBe a [StringValue]
    tbl(1)("f").asInstanceOf[StringValue].value shouldEqual("bar")

    tbl(2)("a") shouldBe a [NumberValue]
    tbl(2)("a").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(2)("e") shouldBe a [NumberValue]
    tbl(2)("e").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(2)("b") shouldBe a [StringValue]
    tbl(2)("b").asInstanceOf[StringValue].value shouldEqual("baz")
    tbl(2)("f") shouldBe a [StringValue]
    tbl(2)("f").asInstanceOf[StringValue].value shouldEqual("baz")

    tbl(3)("a") shouldBe a [NumberValue]
    tbl(3)("a").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(3)("e") shouldBe a [NumberValue]
    tbl(3)("e").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(3)("b") shouldBe a [StringValue]
    tbl(3)("b").asInstanceOf[StringValue].value shouldEqual("foo")
    tbl(3)("f") shouldBe a [StringValue]
    tbl(3)("f").asInstanceOf[StringValue].value shouldEqual("foo")

    tbl(4)("a") shouldBe a [NumberValue]
    tbl(4)("a").asInstanceOf[NumberValue].value shouldEqual(4.0)
    tbl(4)("e") shouldBe a [NumberValue]
    tbl(4)("e").asInstanceOf[NumberValue].value shouldEqual(4.0)
    tbl(4)("b") shouldBe a [StringValue]
    tbl(4)("b").asInstanceOf[StringValue].value shouldEqual("fib")
    tbl(4)("f") shouldBe a [StringValue]
    tbl(4)("f").asInstanceOf[StringValue].value shouldEqual("fib")
  }
}
