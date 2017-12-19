package org.xalgorithms.discover_rules

import java.util.TimeZone

import com.arangodb.spark.ArangoSpark
import com.arangodb.spark.ReadOptions
import config.Settings
import kafka.serializer.StringDecoder
import utils.SparkUtils._
import utils.KafkaSinkUtils._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector._
import org.xalgorithms.discover_rules.udt.Invoice
import com.github.nscala_time.time.Imports._


object Job {
  def filter_items(tuple: (Invoice, CassandraRow)): Boolean = {
    val envelope = tuple._1.envelope
    val rule = tuple._2
    val country = rule.get[String]("country")
    val region = rule.get[String]("region")
    val timezone = rule.get[String]("timezone")
    val starts = rule.get[DateTime]("starts")
    val ends = rule.get[DateTime]("ends")
    val party = rule.get[String]("party")

    return is_valid_country(envelope.country, country) &&
        is_valid_region(envelope.region, region) &&
        is_valid_party(envelope.party, party) &&
        is_valid_start(envelope.period.starts.toDateTime, starts) &&
        is_valid_end(envelope.period.ends.toDateTime, ends)
  }

  def convertToTimezone(d: DateTime, tz: String): DateTime = {
    // TODO: Fix this
    val timezoneId = TimeZone.getTimeZone(tz).getID

    d.withZone(DateTimeZone.forID(timezoneId))
  }

  def is_valid_start(envelope_start: DateTime, start: DateTime): Boolean = {
    if (start == null) {
      return true
    }
    return envelope_start.isAfter(start)
  }

  def is_valid_end(envelope_end: DateTime, end: DateTime): Boolean = {
    if (end == null) {
      return true
    }
    return envelope_end.isBefore(end)
  }

  def is_valid_country(envelope_country: String, country: String): Boolean = {
    if (country == null) {
      return true
    }
    return envelope_country == country
  }

  def is_valid_region(envelope_region: String, region: String): Boolean = {
    if (region == null) {
      return true
    }
    return envelope_region == region
  }

  def is_valid_party(envelope_party: String, party: String): Boolean = {
    if (party == null) {
      return true
    }
    return envelope_party == party
  }

  def main(args: Array[String]) {
    val sc = getSparkContext("DiscoverRulesJob")
    val batchDuration = Seconds(4)
    val ssc = new StreamingContext(sc, batchDuration)

    val kafkaReceiverParams = Map(
      "metadata.broker.list" -> Settings.brokers,
      "group.id" -> "xadf",
      "auto.offset.reset" -> "largest"
    )

    val producerProps = composeProducerConfig(Settings.brokers)
    val kafkaSink = sc.broadcast(producerProps)


    case class test_bean (_key: String){ def this() = this(_key = null) }

    case class rule_bean (_key: String){ def this() = this(_key = null) }

    val ids = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaReceiverParams, Set(Settings.receiver_topic)
    )
      .map(_._2)
      .map({id =>
        "doc._key=='" + id + "'"
      })
      .reduce({(criteria1, criteria2) =>
        criteria1 + "||" + criteria2
      })
      .transform({rdd =>
        val criteria = rdd.collect().mkString("")
        if (criteria.isEmpty()) {
          ArangoSpark.load[Invoice](sc, "invoices", ReadOptions("xadf"))
        } else {
          ArangoSpark.load[Invoice](sc, "invoices", ReadOptions("xadf")).filter(criteria)
        }
      })
      .transform({rdd =>
        val rules = sc.cassandraTable("xadf", "effective_rules")
        rdd.cartesian(rules)
      })
      .transform({rdd =>
        rdd.filter({tuple =>
          filter_items(tuple)
        })
      })
      .foreachRDD { rdd =>
        rdd.foreach { t =>
          kafkaSink.value.send(Settings.producer_topic, t._1._key + ":" + t._2.getString("id"))
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}
