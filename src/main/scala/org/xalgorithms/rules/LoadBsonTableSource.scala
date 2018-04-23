package org.xalgorithms.rules

import collection.JavaConverters._
import org.bson._

import org.xalgorithms.rules.elements._

abstract class LoadBsonTableSource extends LoadTableSource {
  def read(ptref: PackagedTableReference): BsonArray

  def load(ptref: PackagedTableReference): Seq[Map[String, Value]] = {
    make_table(read(ptref))
  }

  def make_table(doc: BsonArray): Seq[Map[String, Value]] = {
    doc.getValues.asScala.map { bv =>
      make_row(bv)
    }
  }

  def make_row(bv: BsonValue): Map[String, Value] = bv match {
    case (d: BsonDocument) => d.keySet.asScala.foldLeft(Map[String, Value]()) { (m, k) =>
      m ++ make_value(k, d.get(k))
    }
    case _ => Map[String, Value]()
  }

  def make_value(k: String, v: BsonValue): Map[String, Value] = v match {
    case (nv: BsonDouble)  => Map(k -> new NumberValue(nv.getValue()))
    case (nv: BsonInt32)  => Map(k -> new NumberValue(nv.doubleValue()))
    case (nv: BsonInt64)  => Map(k -> new NumberValue(nv.doubleValue()))
    case _ => Map(k -> new StringValue(v.asString.getValue()))
  }
}
