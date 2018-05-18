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

import org.bson._
import org.joda.time.DateTime


class Match extends Serializable {
  def match_value(v: BsonValue): Boolean = v.getBsonType() match {
    case BsonType.BOOLEAN  => match_value(v.asInt32())
    case BsonType.DATE_TIME  => match_value(v.asDateTime())
    case BsonType.DOUBLE  => match_value(v.asDouble())
    case BsonType.INT64  => match_value(v.asInt64())
    case BsonType.INT32  => match_value(v.asInt32())
    case BsonType.STRING  => match_value(v.asString())
    case BsonType.TIMESTAMP  => match_value(v.asTimestamp())
// unhandled cases
/*
    case BsonType.ARRAY  => match_value(v.asArray())
    case BsonType.BINARY => match_value(v.asBinary())
    case BsonType.DB_POINTER  => match_value(v.asInt32())
    case BsonType.DECIMAL128  => match_value(v.asInt32())
    case BsonType.DOCUMENT  => match_value(v.asInt32())
    case BsonType.JAVASCRIPT  => match_value(v.asInt32())
    case BsonType.JAVASCRIPT_WITH_SCOPE  => match_value(v.asInt32())
    case BsonType.MAX_KEY  => match_value(v.asInt32())
    case BsonType.MIN_KEY  => match_value(v.asInt32())
    case BsonType.NULL  => match_value(v.asInt32())
    case BsonType.OBJECT_ID  => match_value(v.asInt32())
    case BsonType.REGULAR_EXPRESSION  => match_value(v.asInt32())
    case BsonType.SYMBOL  => match_value(v.asInt32())
    case BsonType.UNDEFINED  => match_value(v.asInt32())
*/
    case default => false
  }

  def match_value(v: BsonDateTime): Boolean = {
    false
  }

  def match_value(v: BsonDouble): Boolean = {
    false
  }

  def match_value(v: BsonInt32): Boolean = {
    false
  }

  def match_value(v: BsonInt64): Boolean = {
    false
  }

  def match_value(v: BsonString): Boolean = {
    false
  }

  def match_value(v: BsonTimestamp): Boolean = {
    false
  }
}

class Equals(value: String) extends Match {
  override def match_value(v: BsonInt32): Boolean = {
    v.getValue() == value.toInt
  }

  override def match_value(v: BsonInt64): Boolean = {
    v.getValue() == value.toInt
  }

  override def match_value(v: BsonDouble): Boolean = {
    v.getValue() == value.toDouble
  }

  override def match_value(v: BsonString): Boolean = {
    v.getValue() == value
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    v.getTime() == value.toInt
  }

  override def match_value(v: BsonDateTime): Boolean = {
    new DateTime(v.getValue).isEqual(new DateTime(value))
  }
}

class LessThan(value: String) extends Match {
  override def match_value(v: BsonInt32): Boolean = {
    v.getValue() < value.toInt
  }

  override def match_value(v: BsonInt64): Boolean = {
    v.getValue() < value.toInt
  }

  override def match_value(v: BsonDouble): Boolean = {
    v.getValue() < value.toDouble
  }

  override def match_value(v: BsonString): Boolean = {
    v.getValue() < value
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    v.getTime() < value.toInt
  }

  override def match_value(v: BsonDateTime): Boolean = {
    new DateTime(v.getValue()).isBefore(new DateTime(value))
  }
}

class LessThanEquals(value: String) extends Match {
  override def match_value(v: BsonInt32): Boolean = {
    v.getValue() <= value.toInt
  }

  override def match_value(v: BsonInt64): Boolean = {
    v.getValue() <= value.toInt
  }

  override def match_value(v: BsonDouble): Boolean = {
    v.getValue() <= value.toDouble
  }

  override def match_value(v: BsonString): Boolean = {
    v.getValue() <= value
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    v.getTime() <= value.toInt
  }

  override def match_value(v: BsonDateTime): Boolean = {
    val dt0 = new DateTime(v.getValue())
    val dt1 = new DateTime(value)

    dt0.isBefore(dt1) || dt0.isEqual(dt1)
  }
}

class GreaterThan(value: String) extends Match {
  override def match_value(v: BsonInt32): Boolean = {
    v.getValue() > value.toInt
  }

  override def match_value(v: BsonInt64): Boolean = {
    v.getValue() > value.toInt
  }

  override def match_value(v: BsonDouble): Boolean = {
    v.getValue() > value.toDouble
  }

  override def match_value(v: BsonString): Boolean = {
    v.getValue() > value
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    v.getTime() > value.toInt
  }

  override def match_value(v: BsonDateTime): Boolean = {
    new DateTime(v.getValue()).isAfter(new DateTime(value))
  }
}

class GreaterThanEquals(value: String) extends Match {
  override def match_value(v: BsonInt32): Boolean = {
    v.getValue() >= value.toInt
  }

  override def match_value(v: BsonInt64): Boolean = {
    v.getValue() >= value.toInt
  }

  override def match_value(v: BsonDouble): Boolean = {
    v.getValue() >= value.toDouble
  }

  override def match_value(v: BsonString): Boolean = {
    v.getValue() >= value
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    v.getTime() >= value.toInt
  }

  override def match_value(v: BsonDateTime): Boolean = {
    val dt0 = new DateTime(v.getValue())
    val dt1 = new DateTime(value)

    dt0.isAfter(dt1) || dt0.isEqual(dt1)
  }
}

object Match {
  def apply(op: String, value: String): Match = op match {
    case "eq"  => new Equals(value)
    case "lt"  => new LessThan(value)
    case "lte" => new LessThanEquals(value)
    case "gt"  => new GreaterThan(value)
    case "gte" => new GreaterThanEquals(value)
  }
}
