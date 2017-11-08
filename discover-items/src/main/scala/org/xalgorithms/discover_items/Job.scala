package org.xalgorithms.discover_items;

import utils.SparkUtils._
import utils.KafkaSinkUtils._
import config.Settings
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
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
        val documentRDD = rdd.map({ids =>
          val document_id = ids.split(":")(0)
          Tuple1(document_id)
        })

        val ruleRDD = rdd.map({ids =>
          val rule_id = ids.split(":")(1)
          Tuple1(rule_id)
        })

        val items = documentRDD.joinWithCassandraTable("xadf", "invoices").select("items")
        val filters = ruleRDD.joinWithCassandraTable("xadf", "rules").select("filters")

        items.cartesian(filters)
      })
      .filter({tuple =>
        // TODO: Filter
        true
      })
      .foreachRDD({rdd =>
        rdd.foreach({tuple =>
          // Dealing with ((document_id, CssandraRow(items)), (rule_id, CassandraRow(rule)))
          val items = tuple._1._2.getList[String]("items")
          val rule_id = tuple._2._1

          items.foreach({item =>
            kafkaSink.value.send(Settings.producer_topic, item + "::" + rule_id)
          })
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
