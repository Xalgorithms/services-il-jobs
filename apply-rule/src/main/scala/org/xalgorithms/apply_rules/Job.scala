package org.xalgorithms.apply_rules;

import config.Settings
import kafka.serializer.StringDecoder
import utils.SparkUtils._
import utils.KafkaSinkUtils._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
import org.xalgorithms.apply_rules.udt._
import org.xalgorithms.apply_rules.udt.MyJsonProtocol._
import spray.json._

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
      rdd.map({source =>
        val jsonAst = source.parseJson

        jsonAst.convertTo[Revision]
      })
    })
    // TODO: Actually apply rules
    .transform({rdd =>
      rdd.foreach({revision =>
        kafkaSink.value.send(Settings.producer_topic, revision.id)
      })
      rdd
    })
    .saveToCassandra("xadf", "revisions")

    ssc.start()
    ssc.awaitTermination()
  }
}
