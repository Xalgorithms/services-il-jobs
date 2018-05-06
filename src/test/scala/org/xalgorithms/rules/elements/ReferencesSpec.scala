package org.xalgorithms.rules.elements

import org.scalamock.scalatest.MockFactory
import org.scalatest._

import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._

class ReferencesSpec extends FlatSpec with Matchers with MockFactory {
  def map_to_expected(m: Map[String, String]): Map[String, Value] = {
    m.map { case (k, v) => (k, new StringValue(v)) }
  }

  "DocumentReferenceValue" should "load map keys from the Context" in {
    val maps = Map(
      "map0" -> Map("a" -> "00", "b" -> "01"),
      "map1" -> Map("a" -> "xx", "b" -> "yy"))
    val ctx = mock[Context]

    maps.foreach { case (name, ex) =>
      val expected = map_to_expected(ex)
      ex.keySet.foreach { k =>
        val ref = new DocumentReferenceValue(name, k)

        (ctx.lookup_in_map _).expects(name, k).returning(new StringValue(ex(k)))

        val v = ref.get(ctx)
        v shouldBe a [StringValue]
        v.asInstanceOf[StringValue].value shouldEqual(ex(k))
      }
    }
  }

  "TableReference" should "load tables from the Context" in {
    val tables = Map(
      "map0" -> Seq(
        Map("a" -> "00", "b" -> "01"),
        Map("a" -> "10", "b" -> "11")),
      "map1" -> Seq(
        Map("A" -> "xx", "B" -> "yy"),
        Map("A" -> "yy", "B" -> "zz")))
    val ctx = mock[Context]

    tables.foreach { case (key, ex) =>
      val expected = ex.map(map_to_expected)
      val ref = new TableReference("tables", key)

      (ctx.lookup_table _).expects("tables", key).returning(expected)
      ref.get(ctx) shouldEqual(expected)
    }
  }

  "DocumentReferenceValue" should "match equivalent references" in {
    val ref0 = new DocumentReferenceValue("a", "x")

    ref0.matches(new DocumentReferenceValue("a", "x"), "eq") shouldEqual(true)
    ref0.matches(new DocumentReferenceValue("a", "y"), "eq") shouldEqual(false)
    ref0.matches(new DocumentReferenceValue("b", "x"), "eq") shouldEqual(false)

    ref0.matches(new DocumentReferenceValue("a", "x"), "lt") shouldEqual(false)
    ref0.matches(new DocumentReferenceValue("a", "y"), "lt") shouldEqual(false)
    ref0.matches(new DocumentReferenceValue("b", "x"), "lt") shouldEqual(false)

    ref0.matches(new DocumentReferenceValue("a", "x"), "lte") shouldEqual(false)
    ref0.matches(new DocumentReferenceValue("a", "y"), "lte") shouldEqual(false)
    ref0.matches(new DocumentReferenceValue("b", "x"), "lte") shouldEqual(false)

    ref0.matches(new DocumentReferenceValue("a", "x"), "gt") shouldEqual(false)
    ref0.matches(new DocumentReferenceValue("a", "y"), "gt") shouldEqual(false)
    ref0.matches(new DocumentReferenceValue("b", "x"), "gt") shouldEqual(false)

    ref0.matches(new DocumentReferenceValue("a", "x"), "gte") shouldEqual(false)
    ref0.matches(new DocumentReferenceValue("a", "y"), "gte") shouldEqual(false)
    ref0.matches(new DocumentReferenceValue("b", "x"), "gte") shouldEqual(false)
  }
}
