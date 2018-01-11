package org.xalgorithms.apply_rules;

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import config.Settings
import kafka.serializer.StringDecoder
import utils.SparkUtils._
import utils.KafkaSinkUtils._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.xalgorithms.apply_rules.udt._
import org.apache.commons.lang3.reflect.FieldUtils


object Job {
  val VALID_SECTIONS = Array(
    "envelope",
    "item"
  )

  def executeRule(doc: Invoice, rule: Rule) = {
    val ruleItems = rule.items
    ruleItems.foreach({r =>
      val whens = r.whens
      val ru = VALID_SECTIONS
        .map({s =>
          (s, getRuleExpression(whens, s))
        })
        .filter({e => e._2 != null})
        .foreach({t =>
          applyExpressions(doc, t._1, t._2)
        })
    })

    doc
  }

  def getRuleExpression(o: WhenRule, field: String): RuleExpression= {
    if (!containsField(o, field)) {
      return null
    }
    val r = FieldUtils.readDeclaredField(o, field, true).asInstanceOf[RuleExpressionWrapper]

    r.expr
  }

  def applyExpressions(d: Invoice, s: String, e: RuleExpression) = {
    val section = getField(d, s)
    val path = e.left.value
    val value = e.right.value
    val op = e.op

    val path_steps = path.split("\\.")

    traverse(d, path_steps, value, op)
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
      FieldUtils.getField(o.getClass, field) != null
    }
    catch {
      case _: Exception=> false
    }
  }

  def getField(o: BaseUDT, field: String): BaseUDT = {
    FieldUtils.readDeclaredField(o, field, true).asInstanceOf[BaseUDT]
  }

  def getStringField(o: BaseUDT, field: String): String = {
    FieldUtils.readDeclaredField(o, field, true).asInstanceOf[String]
  }

  def applyOperator(x: String, y: String, operator: String): Boolean = operator match {
    case "eq" => x == y
    case "neq" => x != y
    case "lt" => x < y
    case "lte" => x <= y
    case "gt" => x > y
    case "gte" => x >= y
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
    .map({id_pair =>
      val parts = id_pair.split(":")
      (parts(0), parts(1))
    })
    .transform({rdd =>
      val docReadConfig = ReadConfig(Map("collection" -> "documents", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
      val ruleReadConfig = ReadConfig(Map("collection" -> "rules", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))

      val count = rdd.count()

      val tuples = rdd.take(count.toInt)

      val res = tuples.map({ids =>
        val document_id = ids._1
        val rule_id = ids._2

        val doc_rd = MongoSpark.load(sc, docReadConfig)
        val doc_ds = doc_rd.toDS[Invoice]()
        doc_ds.filter(doc => doc._id == document_id)

        val rule_rd = MongoSpark.load(sc, docReadConfig)
        val rule_ds = rule_rd.toDS[Rule]()
        rule_ds.filter(rule => rule.rule_id == rule_id)

        (doc_ds.rdd.first(), rule_ds.rdd.first())
      })

      sc.parallelize(res)
    })
    .map({tuple =>
      val doc = tuple._1
      val rule = tuple._2

      executeRule(doc, rule)
    })
    .transform({rdd =>
      // TODO: Store new revision in mongo
      rdd
    })
    .transform({rdd =>
      rdd.foreach({revision =>
        kafkaSink.value.send(Settings.producer_topic, revision._id)
      })
      rdd
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
