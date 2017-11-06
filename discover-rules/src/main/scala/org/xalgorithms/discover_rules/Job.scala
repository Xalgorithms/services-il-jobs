package org.xalgorithms.discover_rules;
import config.Settings
import kafka.serializer.StringDecoder
import utils.SparkUtils._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._

object Job {
  def main(args: Array[String]) {
    val sc = getSparkContext("DiscoverRulesJob")
    val batchDuration = Seconds(4)
    val ssc = new StreamingContext(sc, batchDuration)

    val kafkaParams = Map(
      "metadata.broker.list" -> Settings.brokers,
      "group.id" -> "xadf",
      "auto.offset.reset" -> "largest"
    )

    val ids = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(Settings.topic)
    ).map(_._2).map(id => { Tuple1(id) })

    ids.joinWithCassandraTable("xadf", "invoices").print()

    ssc.start()
    ssc.awaitTermination()
  }
}
