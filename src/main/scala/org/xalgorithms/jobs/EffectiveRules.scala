package org.xalgorithms.jobs

import org.apache.spark.streaming.dstream._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.joda.time.{ DateTime, DateTimeZone }

import org.xalgorithms.apps._

class Envelope(
  val country: Option[String], val region: Option[String],
  val timezone: Option[String], val issued: Option[DateTime]) extends Serializable {
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
  val country: Option[String], val region: Option[String], val timezone: Option[String],
  val starts: Option[DateTime], val ends: Option[DateTime]) extends Serializable {

  def matches(e: Envelope): Boolean = {
    is_global_or_matching_jurisdication(e.country, e.region) &&
      is_within_effective_period(e.issued, e.timezone);
  }

  def is_global_or_matching_jurisdication(
    country: Option[String], region: Option[String]): Boolean = {
    if (None == this.country) {
      // this rule applies anywhere
      true
    } else if (this.country == Some(this.country) && None == this.region) {
      // this rule applies anywhere in the country
      this.country.get == country.get
    }

    // otherwise, must have matching region
    this.country.get == country.get && this.region.get == region.get
  }

  def is_within_effective_period(
    issued: Option[DateTime], timezone: Option[String]): Boolean = {

    if (None == issued || None == timezone || None == this.timezone || (timezone.get != this.timezone.get)) {
      // we're in different timezones, if the rule was created correctly, there should
      // be an Effective for the correct timezone.
      // in the case of missing zones, there's nothing we can do
      // same for if the document has no timezone
      false
    }

    val local_issued = make_local_datetime(issued.get, timezone.get)
    val local_starts = maybe_make_local_datetime(this.starts, this.timezone.get)
    val local_ends = maybe_make_local_datetime(this.ends, this.timezone.get)

    val is_after = (None == local_starts) ||
      local_issued.isEqual(local_starts.get) ||
      local_issued.isAfter(local_starts.get)
    val is_before = (None == local_ends) ||
      local_issued.isEqual(local_ends.get) ||
      local_issued.isBefore(local_ends.get)

    is_after && is_before
  }

  def make_local_datetime(dt: DateTime, tz: String): DateTime = {
    new DateTime(
      dt.year.get, dt.monthOfYear.get, dt.dayOfMonth.get,
      dt.hourOfDay.get, dt.minuteOfHour.get, dt.secondOfMinute.get, dt.millisOfSecond.get,
      DateTimeZone.forID(tz))
  }

  def maybe_make_local_datetime(dt: Option[DateTime], tz: String): Option[DateTime] = {
    if (Some(dt) == dt) Some(make_local_datetime(dt.get, tz)) else None
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
