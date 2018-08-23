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

import org.apache.spark.streaming.dstream._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.joda.time.{ DateTime, DateTimeZone }

import org.xalgorithms.apps._

class Envelope(
  val country: Option[String], val region: Option[String],
  val timezone: Option[String], val issued: Option[DateTime]) extends Serializable {

  def asString(): String = {
    Map(
      "country" -> country, "region" -> region,
      "timezone" -> timezone, "issued" -> issued).foldLeft(Seq[String]()) { case (seq, (k, v)) =>
        if (v.nonEmpty) {
          seq :+ s"${k}=${v.get}"
        } else {
          seq
        }
    }.mkString("; ")
  }
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

  def asString(): String = {
    Map(
      "country" -> country, "region" -> region,
      "timezone" -> timezone, "starts" -> starts, "ends" -> ends).foldLeft(Seq[String]()) { case (seq, (k, v)) =>
        if (v.nonEmpty) {
          seq :+ s"${k}=${v.get}"
        } else {
          seq
        }
    }.mkString("; ")
  }

  def matches(e: Envelope): Boolean = {
    is_global_or_matching_jurisdication(e.country, e.region) &&
      is_within_effective_period(e.issued, e.timezone);
  }

  def is_global_or_matching_jurisdication(
    country: Option[String], region: Option[String]): Boolean = {

    if (None == this.country) {
      // this rule applies anywhere
      return true
    } else if (this.country == Some(this.country) && None == this.region) {
      // this rule applies anywhere in the country
      return this.country.get == country.get
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
      return false
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
    // https://groups.google.com/a/lists.datastax.com/forum/#!topic/spark-connector-user/Uv9UoFjA9SU
    // This discussion lends weight to the observation that the
    // Cassandra driver reads DateTime in LOCALTIME, therefore we need
    // to convert BACK to UTC (we store in UTC) before swapping the
    // TimeZone. The DateTime method used here merely switches the
    // timezone without conversion.
    dt.toDateTime(DateTimeZone.UTC).withZoneRetainFields(DateTimeZone.forID(tz))
  }

  def maybe_make_local_datetime(dt: Option[DateTime], tz: String): Option[DateTime] = {
    if (None == dt ) {
      return None
    }

    Some(make_local_datetime(dt.get, tz))
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

class EffectiveRules(cfg: ApplicationConfig) extends KafkaSparkStreamingApplication(cfg) {
  implicit val job_name: String = "EffectiveRules"

  def execute(): Unit = {
    with_context(cfg, { (ctx, sctx, events, input) =>
      // create a paired dstream on the effective table (K: party, V: (rule_id, Effective))
      events.value.info("starting")
      val effective_paired_stream = new ConstantInputDStream(
        sctx, sctx.cassandraTable(cfg.cassandra_keyspace, "effective")
      ).map { cr => cr.getString("party") -> Tuple2(cr.getString("rule_id"), Effective(cr)) }

      // create a paired dstream on the documents table (K: party, V: (document_id, Envelope)
      // where the document_id == the input document_id
      val document_paired_stream = input.map(Tuple1(_))
        .joinWithCassandraTable(cfg.cassandra_keyspace, "envelopes", AllColumns, SomeColumns("document_id"))
        .map { tup =>
          val party = tup._2.getString("party")
          val document_id = tup._2.getString("document_id")
          val e = Envelope(tup._2)
          events.value.info("mapping input document party to Envelope", Map("party" -> party, "document_id" -> document_id, "envelope" -> e.asString()))
          party -> Tuple2(document_id, e)
        }

      // join the two streams to create (K: party, (Envelope, Effective))
      // similar to inner join on party==party from the two tables
      document_paired_stream.join(effective_paired_stream)
        // just keep the values
        .map(_._2)
        // filter with Effective.matches
        .filter { tup =>
          val matches = tup._2._2.matches(tup._1._2)
          events.value.info("matching envelope to effective", Map("matches" -> matches.toString, "envelope" -> tup._2._2.asString, "effective" -> tup._1._2.asString))
          matches
        }
        // build the result (document_id:rule_id)
        .map { tup =>
          events.value.gave("delivering", Map("document_id" -> tup._1._1, "rule_id" -> tup._2._1))
          tup._1._1 + ":" + tup._2._1
        }
    })
  }
}

object EffectiveRules {
  def execute_job(name: String): Unit = {
    val job = new EffectiveRules(ApplicationConfig(name))
    job.execute()
  }

  def main(args: Array[String]) : Unit = {
    execute_job("EffectiveRules")
  }
}

object ValidateEffectiveRules {
  def main(args: Array[String]) : Unit = {
    EffectiveRules.execute_job("ValidateEffectiveRules")
  }
}
