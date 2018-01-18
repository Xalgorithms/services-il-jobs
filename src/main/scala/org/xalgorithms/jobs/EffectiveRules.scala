package org.xalgorithms.jobs

import org.apache.spark.streaming.dstream._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.joda.time.DateTime

import org.xalgorithms.apps._

class Envelope(
  country: Option[String], region: Option[String],
  timezone: Option[String], issued: Option[DateTime]) extends Serializable {
}

object Envelope {
  def apply(cr: CassandraRow): Envelope = {
    new Envelope(
      cr.getStringOption("country"),
      cr.getStringOption("region"),
      cr.getStringOption("timezone"),
      cr.getDateTimeOption("issued"))
  }
}

class Effective(
  country: Option[String], region: Option[String], timezone: Option[String],
  starts: Option[DateTime], ends: Option[DateTime]) extends Serializable {
  def matches(e: Envelope): Boolean = {
    return true
  }
}

object Effective {
  def apply(cr: CassandraRow): Effective = {
    new Effective(
      cr.getStringOption("country"),
      cr.getStringOption("region"),
      cr.getStringOption("timezone"),
      cr.getDateTimeOption("starts"),
      cr.getDateTimeOption("ends"))
  }
}

class EffectiveRules(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg) {
  def execute(): Unit = {
    with_context(cfg, { (ctx, sctx, input) =>
      // create a paired dstream on the effective table (K: party, V: (rule_id, Effective))
      val effective_paired_stream = new ConstantInputDStream(
        sctx, sctx.cassandraTable("xadf", "effective")
      ).map { cr => cr.getString("party") -> Tuple2(cr.getString("rule_id"), Effective(cr)) }

      // create a paired dstream on the documents table (K: party, V: (document_id, Envelope)
      // where the document_id == the input document_id
      val document_paired_stream = input.map(Tuple1(_))
        .joinWithCassandraTable("xadf", "envelopes", AllColumns, SomeColumns("document_id"))
        .map { tup => tup._2.getString("party") -> Tuple2(tup._2.getString("document_id"), Envelope(tup._2)) }

      // join the two streams to create (K: party, (Envelope, Effective))
      // similar to inner join on party==party from the two tables
      document_paired_stream.join(effective_paired_stream)
        // just keep the values
        .map(_._2)
        // filter with Effective.matches
        .filter { tup => tup._2._2.matches(tup._1._2) }
        // build the result (document_id:rule_id)
        .map { tup =>
          tup._1._1 + ":" + tup._2._1
        }
    })
  }
}

object EffectiveRules {
  def main(args: Array[String]) : Unit = {
    val job = new EffectiveRules(ApplicationConfig("EffectiveRules"))
    job.execute()
  }
}
