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
  def traverse(source: UDTValue, path: Array[String], value: String): Boolean = {
    val p = path.head

    if (!source.columnNames.contains(p)) {
      return true
    }

    if (path.length == 1) {
      return source.getString(p) == value
    }

    val next = source.get[Option[UDTValue]](p)
    if (next.isEmpty) {
      return true
    }

    traverse(next.get, path.drop(1), value)
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
    ).map(_._2).map(Tuple1(_))



    ids.joinWithCassandraTable("xadf", "invoices").map(_._2).transform({rdd =>
      val rules = sc.cassandraTable("xadf", "rules")
      rdd.cartesian(rules)
    }).transform({rdd =>
      rdd.filter({tuple =>
        val filters = tuple._2.get[Option[UDTValue]]("filters")

        filters.isDefined
      }).filter({tuple =>
        val filters = tuple._2.getUDTValue("filters")
        val envelope = filters.get[Option[Set[UDTValue]]]("envelope")

        envelope.isDefined
      }).filter({tuple =>
        val filters = tuple._2.getUDTValue("filters")
        val envelope = filters.get[List[UDTValue]]("envelope")

        val path = envelope.head.getString("path")
        val value = envelope.head.getString("value")

        val path_steps = path.split("\\.")

        traverse(tuple._1.getUDTValue(path_steps.head), path_steps.drop(1), value)
      })
    })
      .foreachRDD { rdd =>
        rdd.foreach { t =>
          kafkaSink.value.send(Settings.producer_topic, t._1.getString("id") + ":" + t._2.getString("id"))
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}
