package org.xalgorithms.rules.elements

import org.scalamock.scalatest.MockFactory
import org.scalatest._

import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._

class ContextSpec extends FlatSpec with Matchers with MockFactory {
  "Context" should "retain maps" in {
    val maps = Map(
      "map0" -> Map("a" -> new StringValue("00"), "b" -> new StringValue("01")),
      "map1" -> Map("A" -> new StringValue("xx"), "B" -> new StringValue("yy")))

    val ctx = new Context(null)

    maps.foreach { case (name, m) =>
      m.keySet.foreach { k =>
        ctx.lookup_in_map(name, k) shouldEqual(null)
      }

      ctx.retain_map(name, m)
      m.keySet.foreach { k =>
        val v = ctx.lookup_in_map(name, k)
        v should not be null
        v shouldBe a [StringValue]
        v.asInstanceOf[StringValue].value shouldEqual(m(k).value)
      }
    }
  }

  it should "retain tables" in {
    val tables = Map(
      "map0" -> Seq(
        Map("a" -> new StringValue("00"), "b" -> new StringValue("01")),
        Map("a" -> new StringValue("10"), "b" -> new StringValue("11"))),
      "map1" -> Seq(
        Map("A" -> new StringValue("xx"), "B" -> new StringValue("yy")),
        Map("A" -> new StringValue("yy"), "B" -> new StringValue("zz"))))

    val ctx = new Context(null)

    tables.foreach { case (name, table) =>
      ctx.lookup_table("tables0", name) shouldEqual(null)
      ctx.lookup_table("tables1", name) shouldEqual(null)

      ctx.retain_table("tables0", name, table)
      ctx.lookup_table("tables0", name) shouldEqual(table)
      ctx.lookup_table("tables1", name) shouldEqual(null)

      ctx.retain_table("tables1", name, table)
      ctx.lookup_table("tables0", name) shouldEqual(table)
      ctx.lookup_table("tables1", name) shouldEqual(table)
    }
  }
}
