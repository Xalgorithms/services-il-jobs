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
import org.joda.time.DateTime
import org.scalatest._

// extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with LoggingTrait
class MatchSpec extends FlatSpec with Matchers {
  // BsonInt32
  "Match" should "indicate whether BsonInt32 equals an input String" in {
    Match("eq", "1").match_value(new BsonInt32(1)) shouldBe true
    Match("eq", "1").match_value(new BsonInt32(2)) shouldBe false
    Match("eq", "3").match_value(new BsonInt32(3)) shouldBe true
    Match("eq", "3").match_value(new BsonInt32(4)) shouldBe false
  }

  it should "indicate whether BsonInt32 is greater than an input String" in {
    Match("gt", "2").match_value(new BsonInt32(1)) shouldBe false
    Match("gt", "2").match_value(new BsonInt32(2)) shouldBe false
    Match("gt", "2").match_value(new BsonInt32(3)) shouldBe true
  }

  it should "indicate whether BsonInt32 is greater than or equal to an input String" in {
    Match("gte", "2").match_value(new BsonInt32(1)) shouldBe false
    Match("gte", "2").match_value(new BsonInt32(2)) shouldBe true
    Match("gte", "2").match_value(new BsonInt32(3)) shouldBe true
  }

  it should "indicate whether BsonInt32 is less than an input String" in {
    Match("lt", "2").match_value(new BsonInt32(1)) shouldBe true
    Match("lt", "2").match_value(new BsonInt32(2)) shouldBe false
    Match("lt", "2").match_value(new BsonInt32(3)) shouldBe false
  }

  it should "indicate whether BsonInt32 is less than or equal to an input String" in {
    Match("lte", "2").match_value(new BsonInt32(1)) shouldBe true
    Match("lte", "2").match_value(new BsonInt32(2)) shouldBe true
    Match("lte", "2").match_value(new BsonInt32(3)) shouldBe false
  }

  // BsonInt64
  it should "indicate whether BsonInt64 equals an input String" in {
    Match("eq", "1").match_value(new BsonInt64(1)) shouldBe true
    Match("eq", "1").match_value(new BsonInt64(2)) shouldBe false
    Match("eq", "3").match_value(new BsonInt64(3)) shouldBe true
    Match("eq", "3").match_value(new BsonInt64(4)) shouldBe false
  }

  it should "indicate whether BsonInt64 is greater than an input String" in {
    Match("gt", "2").match_value(new BsonInt64(1)) shouldBe false
    Match("gt", "2").match_value(new BsonInt64(2)) shouldBe false
    Match("gt", "2").match_value(new BsonInt64(3)) shouldBe true
  }

  it should "indicate whether BsonInt64 is greater than or equal to an input String" in {
    Match("gte", "2").match_value(new BsonInt64(1)) shouldBe false
    Match("gte", "2").match_value(new BsonInt64(2)) shouldBe true
    Match("gte", "2").match_value(new BsonInt64(3)) shouldBe true
  }

  it should "indicate whether BsonInt64 is less than an input String" in {
    Match("lt", "2").match_value(new BsonInt64(1)) shouldBe true
    Match("lt", "2").match_value(new BsonInt64(2)) shouldBe false
    Match("lt", "2").match_value(new BsonInt64(3)) shouldBe false
  }

  it should "indicate whether BsonInt64 is less than or equal to an input String" in {
    Match("lte", "2").match_value(new BsonInt64(1)) shouldBe true
    Match("lte", "2").match_value(new BsonInt64(2)) shouldBe true
    Match("lte", "2").match_value(new BsonInt64(3)) shouldBe false
  }

  // BsonDouble
  it should "indicate whether BsonDouble equals an input String" in {
    Match("eq", "1").match_value(new BsonDouble(1.0)) shouldBe true
    Match("eq", "1.1").match_value(new BsonDouble(1.2)) shouldBe false
    Match("eq", "1.3").match_value(new BsonDouble(1.3)) shouldBe true
    Match("eq", "1.4").match_value(new BsonDouble(1.33)) shouldBe false
  }

  it should "indicate whether BsonDouble is greater than an input String" in {
    Match("gt", "1.2").match_value(new BsonDouble(1.1)) shouldBe false
    Match("gt", "1.2").match_value(new BsonDouble(1.2)) shouldBe false
    Match("gt", "1.2").match_value(new BsonDouble(1.3)) shouldBe true
  }

  it should "indicate whether BsonDouble is greater than or equal to an input String" in {
    Match("gte", "1.2").match_value(new BsonDouble(1.1)) shouldBe false
    Match("gte", "1.2").match_value(new BsonDouble(1.2)) shouldBe true
    Match("gte", "1.2").match_value(new BsonDouble(1.3)) shouldBe true
  }

  it should "indicate whether BsonDouble is less than an input String" in {
    Match("lt", "1.2").match_value(new BsonDouble(1.1)) shouldBe true
    Match("lt", "1.2").match_value(new BsonDouble(1.2)) shouldBe false
    Match("lt", "1.2").match_value(new BsonDouble(1.3)) shouldBe false
  }

  it should "indicate whether BsonDouble is less than or equal to an input String" in {
    Match("lte", "1.2").match_value(new BsonDouble(1.1)) shouldBe true
    Match("lte", "1.2").match_value(new BsonDouble(1.2)) shouldBe true
    Match("lte", "1.2").match_value(new BsonDouble(1.3)) shouldBe false
  }

  // BsonString
  it should "indicate String equivalence" in {
    Match("eq", "aaa").match_value(new BsonString("aaa")) shouldBe true
    Match("eq", "aaa").match_value(new BsonString("abb")) shouldBe false
    Match("eq", "aaa").match_value(new BsonString("AaA")) shouldBe false
  }

  it should "indicate greater than as alphabetical sorting" in {
    Match("gt", "aaa").match_value(new BsonString("zzz")) shouldBe true
    Match("gt", "zzz").match_value(new BsonString("aaa")) shouldBe false
    Match("gt", "aaa").match_value(new BsonString("aaa")) shouldBe false
    Match("gt", "1").match_value(new BsonString("aaa")) shouldBe true
  }

  it should "indicate greater than equals as alphabetical sorting" in {
    Match("gte", "aaa").match_value(new BsonString("zzz")) shouldBe true
    Match("gte", "zzz").match_value(new BsonString("aaa")) shouldBe false
    Match("gte", "aaa").match_value(new BsonString("aaa")) shouldBe true
    Match("gte", "1").match_value(new BsonString("aaa")) shouldBe true
  }

  it should "indicate less than as alphabetical sorting" in {
    Match("lt", "aaa").match_value(new BsonString("zzz")) shouldBe false
    Match("lt", "zzz").match_value(new BsonString("aaa")) shouldBe true
    Match("lt", "aaa").match_value(new BsonString("aaa")) shouldBe false
    Match("lt", "1").match_value(new BsonString("aaa")) shouldBe false
  }

  it should "indicate less than equals as alphabetical sorting" in {
    Match("lte", "aaa").match_value(new BsonString("zzz")) shouldBe false
    Match("lte", "zzz").match_value(new BsonString("aaa")) shouldBe true
    Match("lte", "aaa").match_value(new BsonString("aaa")) shouldBe true
    Match("lte", "1").match_value(new BsonString("aaa")) shouldBe false
  }

  // BsonTimestamp
  it should "indicate whether BsonTimestamp equals an input String" in {
    Match("eq", "1").match_value(new BsonTimestamp(1, 0)) shouldBe true
    Match("eq", "1").match_value(new BsonTimestamp(2, 0)) shouldBe false
    Match("eq", "3").match_value(new BsonTimestamp(3, 0)) shouldBe true
    Match("eq", "3").match_value(new BsonTimestamp(4, 0)) shouldBe false
  }

  it should "indicate whether BsonTimestamp is greater than an input String" in {
    Match("gt", "2").match_value(new BsonTimestamp(1, 0)) shouldBe false
    Match("gt", "2").match_value(new BsonTimestamp(2, 0)) shouldBe false
    Match("gt", "2").match_value(new BsonTimestamp(3, 0)) shouldBe true
  }

  it should "indicate whether BsonTimestamp is greater than or equal to an input String" in {
    Match("gte", "2").match_value(new BsonTimestamp(1, 0)) shouldBe false
    Match("gte", "2").match_value(new BsonTimestamp(2, 0)) shouldBe true
    Match("gte", "2").match_value(new BsonTimestamp(3, 0)) shouldBe true
  }

  it should "indicate whether BsonTimestamp is less than an input String" in {
    Match("lt", "2").match_value(new BsonTimestamp(1, 0)) shouldBe true
    Match("lt", "2").match_value(new BsonTimestamp(2, 0)) shouldBe false
    Match("lt", "2").match_value(new BsonTimestamp(3, 0)) shouldBe false
  }

  it should "indicate whether BsonTimestamp is less than or equal to an input String" in {
    Match("lte", "2").match_value(new BsonTimestamp(1, 0)) shouldBe true
    Match("lte", "2").match_value(new BsonTimestamp(2, 0)) shouldBe true
    Match("lte", "2").match_value(new BsonTimestamp(3, 0)) shouldBe false
  }

  // BsonDateTime
  it should "match equivalent DateTimes" in {
    Match("eq", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+04:00").getMillis())) shouldBe true
    Match("eq", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T02:34:41+04:00").getMillis())) shouldBe false
    Match("eq", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+01:00").getMillis())) shouldBe false
    Match("eq", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+05:00").getMillis())) shouldBe false
    Match("eq", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2019-02-07T03:34:41+04:00").getMillis())) shouldBe false
  }

  it should "match greater than as times after" in {
    Match("gt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+04:00").getMillis())) shouldBe false
    Match("gt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T02:34:41+04:00").getMillis())) shouldBe false
    Match("gt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+01:00").getMillis())) shouldBe true
    Match("gt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+05:00").getMillis())) shouldBe false
    Match("gt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2019-02-07T03:34:41+04:00").getMillis())) shouldBe true
  }

  it should "match greater than equal as equivalent or after" in {
    Match("gte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+04:00").getMillis())) shouldBe true
    Match("gte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T02:34:41+04:00").getMillis())) shouldBe false
    Match("gte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+01:00").getMillis())) shouldBe true
    Match("gte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+05:00").getMillis())) shouldBe false
    Match("gte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2019-02-07T03:34:41+04:00").getMillis())) shouldBe true
  }

  it should "match less than as times before" in {
    Match("lt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+04:00").getMillis())) shouldBe false
    Match("lt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T02:34:41+04:00").getMillis())) shouldBe true
    Match("lt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+01:00").getMillis())) shouldBe false
    Match("lt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+05:00").getMillis())) shouldBe true
    Match("lt", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2019-02-07T03:34:41+04:00").getMillis())) shouldBe false
  }

  it should "match less than equal as equivalent or before" in {
    Match("lte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+04:00").getMillis())) shouldBe true
    Match("lte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T02:34:41+04:00").getMillis())) shouldBe true
    Match("lte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+01:00").getMillis())) shouldBe false
    Match("lte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2018-02-07T03:34:41+05:00").getMillis())) shouldBe true
    Match("lte", "2018-02-07T03:34:41+04:00").match_value(new BsonDateTime(new DateTime("2019-02-07T03:34:41+04:00").getMillis())) shouldBe false
  }
}
