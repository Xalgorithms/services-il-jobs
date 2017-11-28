package org.xalgorithms.discover_items

import utils.SparkUtils._
import utils.KafkaSinkUtils._
import config.Settings
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import com.datastax.spark.connector._
import spray.json._
import DefaultJsonProtocol._


object Job {
  def applyOperator(x: String, y: String, operator: String): Boolean = operator match {
    case "equal" => x == y
    case "not_equal" => x != y
    case "less_than" => x < y
    case "less_than_equal" => x <= y
    case "greater_than" => x > y
    case "greater_than_equal" => x >= y
  }

  def traverse(source: UDTValue, path: Array[String], value: String, operator: String): Boolean = {
    val p = path.head

    if (!source.columnNames.contains(p)) {
      return true
    }

    if (path.length == 1) {
      return applyOperator(source.getString(p), value, operator)
    }

    val next = source.get[Option[UDTValue]](p)
    if (next.isEmpty) {
      return true
    }

    traverse(next.get, path.drop(1), value, operator)
  }

  def filter_items(tuple: ((String, UDTValue), (String, CassandraRow))): Boolean = {
    val filters = tuple._2._2.get[Option[UDTValue]]("filters")

    // Make sure filters is defined
    if (filters.isEmpty) {
      return true
    }
    val envelope = filters.get.get[Option[Set[UDTValue]]]("envelope")

    // Make sure envelope filters are defined
    if (envelope.isEmpty) {
      return true
    }

    val path = envelope.get.head.getString("path")
    val value = envelope.get.head.getString("value")
    val op = envelope.get.head.getString("op")
    val path_steps = path.split("\\.")

    val item = tuple._1._2
    traverse(item, path_steps.drop(1), value, op)
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

      // Join with table and unwrap id
      val items = documentRDD.joinWithCassandraTable("xadf", "invoices").select("items").map({i =>
        (i._1._1, i._2)
      })
      val rules = ruleRDD.joinWithCassandraTable("xadf", "rules").select("filters").map({r =>
        (r._1._1, r._2)
      })
      val expandedItems = items.flatMap({r =>
        val itms = r._2.get[List[UDTValue]]("items")
        itms.map({i =>
          (r._1, i)
        })
      })

      // Example: ((document_id, {item}),(rule_id,CassandraRow{filters: {item: []}))
      expandedItems.cartesian(rules)
    })

    .filter({tuple =>
      filter_items(tuple)
    }).map({tuple =>
      val document_id = tuple._1._1
      val item = tuple._1._2
      val rule_id = tuple._2._1
      // Convert to a (key, list) tuple to be able to reduce items back together
      (document_id + ":" + rule_id, List(item))
    })
    .reduceByKey({(accum, value) =>
      accum ++ value
    })
    // At this point each item is ("document_id:rule_id", List(all items of the document))
    // TODO: Actually apply the rules
    .foreachRDD({rdd =>
      rdd.foreach({item =>
        val Array(document_id, rule_id) = item._1.split(":")
        val res = Map(
          "document_id" -> document_id,
          "rule_id" -> rule_id,
          "items" -> item._2.map(_.toString()).toJson.compactPrint
        )
        println(res.toJson.compactPrint)
        kafkaSink.value.send(Settings.producer_topic, res.toJson.compactPrint)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
