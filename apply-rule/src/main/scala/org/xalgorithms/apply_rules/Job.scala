package org.xalgorithms.apply_rules;

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

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaReceiverParams, Set(Settings.receiver_topic)
    )
      .map(_._2)
      .transform({rdd =>
        val itemRDD = rdd.map({data =>
          data.split("::")(0)
        })
        val ruleRDD = rdd.map({data =>
          val rule_id = data.split("::")(1)
          Tuple1(rule_id)
        })
        val filters = ruleRDD.joinWithCassandraTable("xadf", "rules").select("filters")

        // TODO: Apply the rule on item

        itemRDD
      })
      .foreachRDD({rdd =>
        rdd.foreach({item =>
          kafkaSink.value.send(Settings.producer_topic, item)
        })
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
