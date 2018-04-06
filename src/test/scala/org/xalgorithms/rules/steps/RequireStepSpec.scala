package org.xalgorithms.rules.steps

import org.scalatest._
import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._
import org.xalgorithms.rules.steps._

import play.api.libs.json._
import scala.io.Source

class RequireSpec extends FlatSpec with Matchers {
  class ResourceLoadTableSource extends LoadTableSource {
    def read(ptref: PackagedTableReference): JsValue = {
      return Json.parse(Source.fromURL(getClass.getResource(s"/${ptref.name}.json")).mkString)
    }
  }

  "RequireStep" should "load tables into the Context" in {
    val load = new ResourceLoadTableSource()
    val ctx = new Context(load)

    val ref = new PackagedTableReference("package", "table0", "0.0.1", "table0")
    val step = new RequireStep(ref, Seq())

    step.execute(ctx)

    val tbl = ctx.find_in_section("table", "table0")

    tbl should not be null
    tbl.length shouldEqual(3)
    tbl(0)("a") shouldBe a [NumberValue]
    tbl(0)("a").asInstanceOf[NumberValue].value shouldEqual(1.0)
    tbl(0)("b") shouldBe a [StringValue]
    tbl(0)("b").asInstanceOf[StringValue].value shouldEqual("foo")
    tbl(1)("a") shouldBe a [NumberValue]
    tbl(1)("a").asInstanceOf[NumberValue].value shouldEqual(2.0)
    tbl(1)("b") shouldBe a [StringValue]
    tbl(1)("b").asInstanceOf[StringValue].value shouldEqual("bar")
    tbl(2)("a") shouldBe a [NumberValue]
    tbl(2)("a").asInstanceOf[NumberValue].value shouldEqual(3.0)
    tbl(2)("b") shouldBe a [StringValue]
    tbl(2)("b").asInstanceOf[StringValue].value shouldEqual("baz")
  }
}
