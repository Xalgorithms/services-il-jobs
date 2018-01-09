package org.xalgorithms.discover_items

import com.mongodb.spark.MongoSpark
import utils.SparkUtils._
import utils.KafkaSinkUtils._
import config.Settings
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import com.datastax.spark.connector._
import com.mongodb.spark.config.ReadConfig
import org.apache.commons.lang3.reflect.FieldUtils
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import org.xalgorithms.discover_items.udt._


object Job {
  def applyOperator(x: String, y: String, operator: String): Boolean = operator match {
    case "equal" => x == y
    case "not_equal" => x != y
    case "less_than" => x < y
    case "less_than_equal" => x <= y
    case "greater_than" => x > y
    case "greater_than_equal" => x >= y
  }

  def section_exists(source: BaseUDT, section: String, path: String): Boolean = {
    val parts = path.split("\\.")
    if (!containsField(source, section)) {
      return false
    }
    val data = getField(source, section)
    path_exists(data, parts)
  }

  def path_exists(source: BaseUDT, path: Array[String]): Boolean = {
    val p = path.head

    if (!containsField(source, p)) {
      return false
    }

    if (path.length == 1) {
      return containsField(source, p)
    }

    val next = getField(source, p)
    if (next == null) {
      return false
    }

    path_exists(next, path.drop(1))
  }

  def traverse(source: BaseUDT, path: Array[String], value: String, operator: String): Boolean = {
    val p = path.head

    if (!containsField(source, p)) {
      return true
    }

    if (path.length == 1) {
      return applyOperator(getStringField(source, p), value, operator)
    }

    val next = getField(source, p)
    if (next == null) {
      return true
    }

    traverse(next, path.drop(1), value, operator)
  }

  def containsField(o: BaseUDT, field: String): Boolean = {
    try {
      FieldUtils.getField(o.getClass, field, true) != null
    }
    catch {
      case _: Exception=> false
    }
  }

  def getStringField(o: BaseUDT, field: String): String = {
    FieldUtils.readDeclaredField(o, field).asInstanceOf[String]
  }

  def getField(o: BaseUDT, field: String): BaseUDT = {
    FieldUtils.readDeclaredField(o, field, true).asInstanceOf[BaseUDT]
  }

  def filter_items(tuple: ((String, Item), (String, CassandraRow))): Boolean = {
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

  def filter_all(tuple: (InvoiceAndWhensKeys, CassandraRow)): Boolean = {
    val doc = tuple._1.document
    val section = tuple._1.section
    val key = tuple._1.key

    val path_steps = key.split("\\.")
    val op = tuple._2.getString("op")
    val value = tuple._2.getString("val")

    traverse(doc, path_steps, value, op)
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

    val readConfig = ReadConfig(Map("collection" -> "documents", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val rd = MongoSpark.load[Invoice](sc, readConfig)

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaReceiverParams, Set(Settings.receiver_topic)
    )
      .map(_._2)
      .map({id_pair =>
        val parts = id_pair.split(":")
        (parts(0), parts(1))
      })

      .transform({rdd =>
        val readConfig = ReadConfig(Map("collection" -> "documents", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))

        val count = rdd.count()

        val tuples = rdd.take(count.toInt)

        val res = tuples.map({ids =>
          val document_id = ids._1
          val rule_id = ids._2

          val rd = MongoSpark.load(sc, readConfig)

          val ds = rd.toDS[Invoice]()
          ds.filter(doc => doc._id == document_id)

          (ds.rdd.first(), rule_id)
        })

        sc.parallelize(res)
      })


      .transform({rdd =>
        val when_keys = sc.cassandraTable[WhenKeys]("xadf", "when_keys")
        val tuple = rdd.cartesian(when_keys)

        tuple.filter({rdd =>
          val paths = rdd._2
          val section = paths.section
          val key = paths.key

          section_exists(rdd._1._1, section, key)
        })
      })
      .map({d =>
        InvoiceAndWhensKeys(d._2.section, d._2.key, d._1._1)
      })
      .transform({rdd =>
        rdd.joinWithCassandraTable("xadf", "whens")
      })
      .filter({tuple =>
        filter_all(tuple)
      })
      .foreachRDD({rdd =>
        rdd.foreach({it =>
          val items = it._1.document.items
          val rule_id = it._2.getString("rule_id")
          val document_id = it._1.document._id

          val res = Map(
            "id" -> document_id,
            "rule_id" -> rule_id,
            "items" -> items
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
