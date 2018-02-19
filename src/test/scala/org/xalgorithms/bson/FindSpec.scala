package org.xalgorithms.bson

import org.xalgorithms.bson._

import org.bson._
import org.scalatest._

class FindSpec extends FlatSpec with Matchers {
  "Find" should "extract values from BsonDocument by path" in {
    val JSON = """{
      "a" : { "aa" : 1, "ab" : "2" },
      "b" : "3",
      "c" : 1
    }"""

    val doc = BsonDocument.parse(JSON)
    Find(doc, "a") shouldBe BsonDocument.parse("""{ "aa" : 1, "ab" : "2" }""")
    Find(doc, "a.aa") shouldBe new BsonInt32(1)
    Find(doc, "a.ab") shouldBe new BsonString("2")
    Find(doc, "b") shouldBe new BsonString("3")
    Find(doc, "c") shouldBe new BsonInt32(1)
  }

  it should "yield null if the path does not exist" in {
    val doc = BsonDocument.parse("{}")
    Find(doc, "a") shouldBe null
    Find(doc, "a.aa") shouldBe null
    Find(doc, "a.ab") shouldBe null
    Find(doc, "b") shouldBe null
    Find(doc, "c") shouldBe null
  }
}
