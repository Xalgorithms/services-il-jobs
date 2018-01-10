package org.xalgorithms.discover_rules

import config.Settings
import kafka.serializer.StringDecoder
import utils.SparkUtils._
import utils.KafkaSinkUtils._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.joda.time.DateTime
import org.xalgorithms.discover_rules.udt._
import com.github.nscala_time.time.Imports._
import java.util.TimeZone


object Job {
  def filter_items(tuple: (Invoice, Effective)): Boolean = {
    val envelope = tuple._1.envelope
    val effective = tuple._2
    val country = effective.country
    val region = effective.region
    val party = effective.party
    val timezone = effective.timezone
    val starts = effective.starts
    val ends = effective.ends

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

    val ids = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaReceiverParams, Set(Settings.receiver_topic)
    ).map(_._2)


    ids
      .transform({rdd =>
        val readConfig = ReadConfig(Map("collection" -> "documents", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))

        val count = rdd.count()

        val ids = rdd.take(count.toInt)


        val res = ids.map({id =>
          val rd = MongoSpark.load(sc, readConfig)

          val ds = rd.toDS[Invoice]()
          ds.filter(doc => doc._id == id)

          ds.rdd.first()
        })

        sc.parallelize(res)
      })
      .transform({rdd =>
        val effective = sc.cassandraTable[Effective]("xadf", "effective")

        rdd.cartesian(effective)
      })
      .transform({rdd =>
        rdd.filter({tuple =>
          filter_items(tuple)
        })
      })
      .foreachRDD { rdd =>
        rdd.foreach { t =>
          kafkaSink.value.send(Settings.producer_topic, t._1._id + ":" + t._2.rule_id)
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}
