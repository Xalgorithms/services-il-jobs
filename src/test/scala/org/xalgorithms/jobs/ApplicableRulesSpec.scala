// Copyright (C) 2018 Don Kelly <karfai@gmail.com>
// Copyright (C) 2018 Hayk Pilosyan <hayk.pilos@gmail.com>

// This file is part of Interlibr, a functional component of an
// Internet of Rules (IoR).

// ACKNOWLEDGEMENTS
// Funds: Xalgorithms Foundation
// Collaborators: Don Kelly, Joseph Potvin and Bill Olders.

// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see
// <http://www.gnu.org/licenses/>.
package org.xalgorithms.jobs

import org.xalgorithms.jobs.ApplicableRules

import org.bson._
import org.scalatest._

class ApplicableRuleSpec extends FlatSpec with Matchers {
  "DocumentValues" should "extract values from the envelope section" in {
    val JSON = """{
      "envelope" : {
        "a" : 1,
        "b" : { "ba" : 2, "bb" : "3" }
      }
    }"""

    val exs = Map(
      "a" -> Seq(new BsonInt32(1)),
      "b.ba" -> Seq(new BsonInt32(2)),
      "b.bb" -> Seq(new BsonString("3"))
    )
    val doc = BsonDocument.parse(JSON)

    exs.foreach { case (path, ex) =>
      DocumentValue(doc, "envelope", path).find() shouldBe ex
    }
  }

  it should "handle missing envelope section" in {
    val doc = BsonDocument.parse("{}")
    val arr = DocumentValue(doc, "envelope", "a").find()
    arr.length shouldBe 0
  }

  it should "handle a mistyped envelope section" in {
    val doc = BsonDocument.parse("""{ "envelope" : "foo" }""")
    val arr = DocumentValue(doc, "envelope", "a").find()
    arr.length shouldBe 0
  }

  it should "extract values from the documents in the items section" in {
    val JSON = """{
      "items" : [
        { "a": "00", "b": "01", "c": "02" },
        "",
        { "a": "10", "c": "12" },
        { "b": "21", c: "22" },
        [0, 1, 2]
      ]
    }"""

    val exs = Map(
      "a" -> Seq(new BsonString("00"), new BsonString("10")),
      "b" -> Seq(new BsonString("01"), new BsonString("21")),
      "c" -> Seq(new BsonString("02"), new BsonString("12"), new BsonString("22"))
    )

    val doc = BsonDocument.parse(JSON)

    exs.foreach { case (path, ex) =>
      DocumentValue(doc, "items", path).find() shouldBe ex
    }
  }

  it should "handle missing items section" in {
    val doc = BsonDocument.parse("{}")
    val arr = DocumentValue(doc, "items", "a").find()
    arr.length shouldBe 0
  }

  it should "handle a mistyped items section" in {
    val doc = BsonDocument.parse("""{ "items" : "foo" }""")
    val arr = DocumentValue(doc, "items", "a").find()
    arr.length shouldBe 0
  }

  "ApplyOperator" should "match if a Seq element matches op, val" in {
    val exs = Seq(
      ("eq", "a", Seq(new BsonString("x"), new BsonString("a"))),
      ("lt", "10", Seq(new BsonInt32(11), new BsonInt32(6))),
      ("gt", "10", Seq(new BsonInt32(11), new BsonInt32(6))),
      ("lte", "10", Seq(new BsonInt32(11), new BsonInt32(10))),
      ("gte", "10", Seq(new BsonInt32(6), new BsonInt32(10)))
    )

    exs.foreach { tup =>
      ApplyOperator(tup._1, tup._2).matches_any(tup._3) shouldBe true
    }
  }

  it should "not match if a Seq element does not match op, val" in {
    val exs = Seq(
      ("eq", "a", Seq(new BsonString("x"), new BsonString("y"))),
      ("lt", "10", Seq(new BsonInt32(11), new BsonInt32(10))),
      ("gt", "10", Seq(new BsonInt32(8), new BsonInt32(10))),
      ("lte", "10", Seq(new BsonInt32(11), new BsonInt32(12))),
      ("gte", "10", Seq(new BsonInt32(6), new BsonInt32(8)))
    )

    exs.foreach { tup =>
      ApplyOperator(tup._1, tup._2).matches_any(tup._3) shouldBe false
    }
  }
}
