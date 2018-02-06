package org.xalgorithms.bson

import org.bson._
import org.joda.time.DateTime


class Match {
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
    value.toInt == v.getValue()
  }

  override def match_value(v: BsonInt64): Boolean = {
    value.toInt == v.getValue()
  }

  override def match_value(v: BsonDouble): Boolean = {
    value.toDouble == v.getValue()
  }

  override def match_value(v: BsonString): Boolean = {
    value == v.getValue()
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    value.toInt == v.getTime()
  }

  override def match_value(v: BsonDateTime): Boolean = {
    new DateTime(value).isEqual(new DateTime(v.getValue))
  }
}

class LessThan(value: String) extends Match {
  override def match_value(v: BsonInt32): Boolean = {
    value.toInt < v.getValue()
  }

  override def match_value(v: BsonInt64): Boolean = {
    value.toInt < v.getValue()
  }

  override def match_value(v: BsonDouble): Boolean = {
    value.toDouble < v.getValue()
  }

  override def match_value(v: BsonString): Boolean = {
    value < v.getValue()
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    value.toInt < v.getTime()
  }

  override def match_value(v: BsonDateTime): Boolean = {
    new DateTime(value).isBefore(new DateTime(v.getValue))
  }
}

class LessThanEquals(value: String) extends Match {
  override def match_value(v: BsonInt32): Boolean = {
    value.toInt <= v.getValue()
  }

  override def match_value(v: BsonInt64): Boolean = {
    value.toInt <= v.getValue()
  }

  override def match_value(v: BsonDouble): Boolean = {
    value.toDouble <= v.getValue()
  }

  override def match_value(v: BsonString): Boolean = {
    value <= v.getValue()
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    value.toInt <= v.getTime()
  }

  override def match_value(v: BsonDateTime): Boolean = {
    val dt0 = new DateTime(value)
    val dt1 = new DateTime(v.getValue)

    dt0.isBefore(dt1) || dt0.isEqual(dt1)
  }
}

class GreaterThan(value: String) extends Match {
  override def match_value(v: BsonInt32): Boolean = {
    value.toInt > v.getValue()
  }

  override def match_value(v: BsonInt64): Boolean = {
    value.toInt > v.getValue()
  }

  override def match_value(v: BsonDouble): Boolean = {
    value.toDouble > v.getValue()
  }

  override def match_value(v: BsonString): Boolean = {
    value > v.getValue()
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    value.toInt > v.getTime()
  }

  override def match_value(v: BsonDateTime): Boolean = {
    new DateTime(value).isAfter(new DateTime(v.getValue))
  }
}

class GreaterThanEquals(value: String) extends Match {
  override def match_value(v: BsonInt32): Boolean = {
    value.toInt >= v.getValue()
  }

  override def match_value(v: BsonInt64): Boolean = {
    value.toInt >= v.getValue()
  }

  override def match_value(v: BsonDouble): Boolean = {
    value.toDouble >= v.getValue()
  }

  override def match_value(v: BsonString): Boolean = {
    value >= v.getValue()
  }

  override def match_value(v: BsonTimestamp): Boolean = {
    value.toInt >= v.getTime()
  }

  override def match_value(v: BsonDateTime): Boolean = {
    val dt0 = new DateTime(value)
    val dt1 = new DateTime(v.getValue)

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
