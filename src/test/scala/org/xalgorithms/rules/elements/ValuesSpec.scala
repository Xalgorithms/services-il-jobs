package org.xalgorithms.rules.elements

import org.scalamock.scalatest.MockFactory
import org.scalatest._

import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._

class ValuesSpec extends FlatSpec with Matchers with MockFactory {
  "NumberValue" should "match or convert" in {
    Seq(
      Tuple4(1.0, new NumberValue(1.0), "eq", true),
      Tuple4(1.0, new NumberValue(2.0), "lt", true),
      Tuple4(2.0, new NumberValue(2.0), "lt", false),
      Tuple4(1.0, new NumberValue(2.0), "lte", true),
      Tuple4(2.0, new NumberValue(2.0), "lte", true),
      Tuple4(3.0, new NumberValue(2.0), "gte", true),
      Tuple4(2.0, new NumberValue(2.0), "gte", true),
      Tuple4(3.0, new NumberValue(2.0), "gt", true),
      Tuple4(2.0, new NumberValue(2.0), "gt", false),
      Tuple4(1.0, new StringValue("1.0"), "eq", true),
      Tuple4(1.0, new StringValue("2.0"), "lt", true),
      Tuple4(2.0, new StringValue("2.0"), "lt", false),
      Tuple4(1.0, new StringValue("2.0"), "lte", true),
      Tuple4(2.0, new StringValue("2.0"), "lte", true),
      Tuple4(3.0, new StringValue("2.0"), "gte", true),
      Tuple4(2.0, new StringValue("2.0"), "gte", true),
      Tuple4(3.0, new StringValue("2.0"), "gt", true),
      Tuple4(2.0, new StringValue("2.0"), "gt", false)
    ).foreach { case (n, rhs, op, ex) =>
        new NumberValue(n).matches(rhs, op) shouldEqual(ex)
    }
  }

  it should "perform addition of basic Values as SUM" in {
    Seq(
      Tuple3(1.0, Seq(new NumberValue(2.2), new NumberValue(3.3), new NumberValue(4.4)), 10.9),
      Tuple3(1.0, Seq(new NumberValue(-2.0), new NumberValue(3.0)), 2.0),
      Tuple3(11.0, Seq(new NumberValue(1.0), new NumberValue(2.0)), 14.0),
      Tuple3(11.0, Seq(new StringValue("1.0"), new NumberValue(2.0)), 14.0),
      Tuple3(11.0, Seq(new StringValue("1.0"), new StringValue("abc"), new StringValue("2.0")), 14.0)
    ).foreach { case (bv, args, ex) =>
        val v = new NumberValue(bv)
        val sum = v.apply_func(args, "add")

        sum match {
          case Some(v) =>
            v shouldBe a [NumberValue]
            v.asInstanceOf[NumberValue].value shouldEqual(ex)
          case None =>
            true shouldEqual(false)
        }
    }
  }

  "StringValue" should "match or convert" in {
    Seq(
      Tuple4("bb", new StringValue("bb"), "eq", true),
      Tuple4("bb", new StringValue("bb"), "lte", true),
      Tuple4("bb", new StringValue("bb"), "gte", true),
      Tuple4("bb", new StringValue("bb"), "lt", false),
      Tuple4("bb", new StringValue("bb"), "gt", false),
      Tuple4("aa", new StringValue("bb"), "lt", true),
      Tuple4("cc", new StringValue("bb"), "gt", true),
      Tuple4("aa", new StringValue("bb"), "gt", false),
      Tuple4("cc", new StringValue("bb"), "lt", false),
      Tuple4("1.0", new NumberValue(1.0), "eq", true),
      Tuple4("1.0", new NumberValue(2.0), "lt", true),
      Tuple4("2.0", new NumberValue(2.0), "lt", false),
      Tuple4("1.0", new NumberValue(2.0), "lte", true),
      Tuple4("2.0", new NumberValue(2.0), "lte", true),
      Tuple4("3.0", new NumberValue(2.0), "gte", true),
      Tuple4("2.0", new NumberValue(2.0), "gte", true),
      Tuple4("3.0", new NumberValue(2.0), "gt", true),
      Tuple4("2.0", new NumberValue(2.0), "gt", false)
    ).foreach { case (n, rhs, op, ex) =>
        new StringValue(n).matches(rhs, op) shouldEqual(ex)
    }
  }


  it should "perform addition of basic Values as CONCAT" in {
    Seq(
      Tuple3("a", Seq(new StringValue("b"), new StringValue("c")), "abc"),
      Tuple3("a", Seq(new StringValue("1.0"), new NumberValue(2.0)), "a1.02.0"),
      Tuple3("a", Seq(new StringValue("1.0"), new StringValue("abc"), new NumberValue(2.0)), "a1.0abc2.0")
    ).foreach { case (bv, args, ex) =>
        val v = new StringValue(bv)
        val sum = v.apply_func(args, "add")

        sum match {
          case Some(v) =>
            v shouldBe a [StringValue]
            v.asInstanceOf[StringValue].value shouldEqual(ex)
          case None =>
            true shouldEqual(false)
        }
    }
  }

  def match_nothing(mv: Value) {
    Seq(
      Tuple2(new StringValue("bb"), "eq"),
      Tuple2(new StringValue("bb"), "lte"),
      Tuple2(new StringValue("bb"), "gte"),
      Tuple2(new StringValue("bb"), "lt"),
      Tuple2(new StringValue("bb"), "gt"),
      Tuple2(new StringValue("bb"), "lt"),
      Tuple2(new StringValue("bb"), "gt"),
      Tuple2(new StringValue("bb"), "gt"),
      Tuple2(new StringValue("bb"), "lt"),
      Tuple2(new NumberValue(1.0), "eq"),
      Tuple2(new NumberValue(2.0), "lt"),
      Tuple2(new NumberValue(2.0), "lt"),
      Tuple2(new NumberValue(2.0), "lte"),
      Tuple2(new NumberValue(2.0), "lte"),
      Tuple2(new NumberValue(2.0), "gte"),
      Tuple2(new NumberValue(2.0), "gte"),
      Tuple2(new NumberValue(2.0), "gt"),
      Tuple2(new NumberValue(2.0), "gt")).foreach { case (v, op) =>
        mv.matches(v, op) shouldEqual(false)
    }
  }

  "FunctionValue" should "match nothing" in {
    match_nothing(new FunctionValue("foo", Seq()))
  }

  "EmptyValue" should "match nothing" in {
    match_nothing(new EmptyValue())
  }

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

        (ctx.lookup_in_map _).expects(name, k).returning(new StringValue(ex(k))).twice

        var v = ref.resolve(ctx)
        v should not be null
        v shouldBe a [StringValue]
        v.asInstanceOf[StringValue].value shouldEqual(ex(k))

        // try again using ResolveValue
        v = ResolveValue(ref, ctx)
        v should not be null
        v shouldBe a [StringValue]
        v.asInstanceOf[StringValue].value shouldEqual(ex(k))
      }
    }
  }

  it should "match equivalent references" in {
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
