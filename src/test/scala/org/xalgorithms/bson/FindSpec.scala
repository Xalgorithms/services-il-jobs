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
