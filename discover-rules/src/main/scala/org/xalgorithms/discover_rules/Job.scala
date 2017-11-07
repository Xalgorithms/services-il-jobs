package org.xalgorithms.discover_rules;

import config.Settings
import kafka.serializer.StringDecoder
import utils.SparkUtils._
import utils.KafkaSinkUtils._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._

object Job {
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
    ).map(_._2).map(Tuple1(_))

    val rules = sc.cassandraTable("xadf", "rules")

    ids.joinWithCassandraTable("xadf", "invoices").map(_._2).transform({rdd =>
      rdd.cartesian(rules)
    }).filter({tuple =>
      // TODO: Filter rules based on envelope
      true
    }).foreachRDD { rdd =>
      rdd.foreach { t =>
        kafkaSink.value.send(Settings.producer_topic, t._1.getString("id") + ":" + t._2.getString("id"))
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
