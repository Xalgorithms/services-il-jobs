package org.xalgorithms.rules.elements

import org.scalamock.scalatest.MockFactory
import org.scalatest._

import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._

class WhenSpec extends FlatSpec with Matchers with MockFactory {
  "When" should "evaluate simple values" in {
    val ctx = mock[Context]

    Seq("eq", "gt", "gte", "lt", "lte").foreach { op =>
      val v0 = mock[Value]
      val v1 = mock[Value]

      (v0.matches _).expects(v1, op).returning(false)
      (v1.matches _).expects(v0, op).returning(true)

      new When(v0, v1, op).evaluate(ctx) shouldEqual(false)
      new When(v1, v0, op).evaluate(ctx) shouldEqual(true)
    }
  }

  it should "evaluate ReferenceValues" in {
    val ctx = mock[Context]

    Seq("eq", "gt", "gte", "lt", "lte").foreach { op =>
      val v0 = mock[Value]
      val v1 = mock[Value]
      val rv0 = mock[ReferenceValue]
      val rv1 = mock[ReferenceValue]

      (rv0.resolve _).expects(ctx).returning(v0).twice
      (rv1.resolve _).expects(ctx).returning(v1).twice

      (v0.matches _).expects(v1, op).returning(false)
      (v1.matches _).expects(v0, op).returning(true)

      new When(rv0, rv1, op).evaluate(ctx) shouldEqual(false)
      new When(rv1, rv0, op).evaluate(ctx) shouldEqual(true)
    }
  }
}
