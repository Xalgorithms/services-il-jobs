package org.xalgorithms.apply_rules;

import config.Settings
import kafka.serializer.StringDecoder
import utils.SparkUtils._
import utils.KafkaSinkUtils._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
import spray.json.DefaultJsonProtocol


case class Amount(value: BigDecimal, currency_code: String)
case class Measure(value: BigDecimal, unit: String)
case class Pricing(orderable_factor: BigDecimal, price: Amount, quantity: Measure)
case class TaxComponent(amount: Amount, taxable: Amount)
case class ItemTax(total: Amount, components: List[TaxComponent])
case class Item(id: String, price: Amount, quantity: Measure, pricing: Pricing, tax: String)
case class Revision(id: String, items: List[Item])

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val amountFormat = jsonFormat2(Amount)
  implicit val measureFormat = jsonFormat2(Measure)
  implicit val pricingFormat = jsonFormat3(Pricing)
  implicit val itemFormat = jsonFormat5(Item)
  implicit val objFormat = jsonFormat2(Revision)
}

import MyJsonProtocol._
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
      .saveToCassandra("xadf", "revisions")

    ssc.start()
    ssc.awaitTermination()
  }
}
