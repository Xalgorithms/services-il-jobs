package org.xalgorithms.rules.elements

import org.scalamock.scalatest.MockFactory
import org.scalatest._

import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._

class ReferencesSpec extends FlatSpec with Matchers with MockFactory {
  "MakeReference" should "yield a DocumentValueReference in the case of the envelope section" in {
    val ref = MakeReference("envelope", "foo")

    ref shouldBe a [DocumentValueReference]
    ref.section shouldEqual("envelope")
    ref.key shouldEqual("foo")
  }

  it should "yield a TableReference in the case of other sections" in {
    Seq("section0", "section1", "section2").foreach { section =>
      val ref = MakeReference(section, "foo")

      ref shouldBe a [TableReference]
      ref.section shouldEqual(section)
      ref.key shouldEqual("foo")
    }
  }

  def map_to_expected(m: Map[String, String]): Map[String, Value] = {
    m.map { case (k, v) => (k, new StringValue(v)) }
  }

  "DocumentValueReference" should "load map keys from the Context" in {
    val maps = Map(
      "map0" -> Map("a" -> "00", "b" -> "01"),
      "map1" -> Map("a" -> "xx", "b" -> "yy"))
    val ctx = mock[Context]

    maps.foreach { case (name, ex) =>
      val expected = map_to_expected(ex)
      ex.keySet.foreach { k =>
        val ref = new DocumentValueReference(name, k)

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

  "Reference" should "match equivalent references" in {
    val ref0 = new Reference("a", "x")

    ref0.matches(new Reference("a", "x"), "eq") shouldEqual(true)
    ref0.matches(new Reference("a", "y"), "eq") shouldEqual(false)
    ref0.matches(new Reference("b", "x"), "eq") shouldEqual(false)

    ref0.matches(new Reference("a", "x"), "lt") shouldEqual(false)
    ref0.matches(new Reference("a", "y"), "lt") shouldEqual(false)
    ref0.matches(new Reference("b", "x"), "lt") shouldEqual(false)

    ref0.matches(new Reference("a", "x"), "lte") shouldEqual(false)
    ref0.matches(new Reference("a", "y"), "lte") shouldEqual(false)
    ref0.matches(new Reference("b", "x"), "lte") shouldEqual(false)

    ref0.matches(new Reference("a", "x"), "gt") shouldEqual(false)
    ref0.matches(new Reference("a", "y"), "gt") shouldEqual(false)
    ref0.matches(new Reference("b", "x"), "gt") shouldEqual(false)

    ref0.matches(new Reference("a", "x"), "gte") shouldEqual(false)
    ref0.matches(new Reference("a", "y"), "gte") shouldEqual(false)
    ref0.matches(new Reference("b", "x"), "gte") shouldEqual(false)
  }

  "TableReference" should "match equivalent table references" in {
    val ref0 = new TableReference("a", "x")

    ref0.matches(new TableReference("a", "x"), "eq") shouldEqual(true)
    ref0.matches(new DocumentValueReference("a", "x"), "eq") shouldEqual(false)
    ref0.matches(new Reference("a", "x"), "eq") shouldEqual(false)
  }

  "DocumentValueReference" should "match equivalent map references" in {
    val ref0 = new DocumentValueReference("a", "x")

    ref0.matches(new DocumentValueReference("a", "x"), "eq") shouldEqual(true)
    ref0.matches(new TableReference("a", "x"), "eq") shouldEqual(false)
    ref0.matches(new Reference("a", "x"), "eq") shouldEqual(false)
  }
}
