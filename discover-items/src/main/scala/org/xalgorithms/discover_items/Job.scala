package org.xalgorithms.discover_items;

import utils.SparkUtils._
import utils.KafkaSinkUtils._
import config.Settings
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._

import org.json4s._
import org.json4s.native.Serialization._
import org.json4s.native.Serialization

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

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaReceiverParams, Set(Settings.receiver_topic)
    )
      .map(_._2)
      .transform({rdd =>
        val documentRDD = rdd.map({id_pair =>
          val document_id = id_pair.split(":")(0)
          Tuple1(document_id)
        })

        val ruleRDD = rdd.map({id_pair =>
          val rule_id = id_pair.split(":")(1)
          Tuple1(rule_id)
        })

        val items = documentRDD.joinWithCassandraTable("xadf", "invoices").select("items")
        val rules = ruleRDD.joinWithCassandraTable("xadf", "rules").select("filters")

        // TODO: Modify items based on rules
        items
      })
      .foreachRDD({rdd =>
        rdd.foreach({item =>
          val res = Map(
            "id" -> item._1._1,
            "items" -> item._2.getString("items")
          )
          implicit val formats = Serialization.formats(NoTypeHints)

          val jsonStr = write(res)
          kafkaSink.value.send(Settings.producer_topic, jsonStr)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
