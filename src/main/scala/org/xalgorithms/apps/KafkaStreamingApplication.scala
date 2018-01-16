package org.xalgorithms.apps

import kafka.serializer.StringDecoder
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

class KafkaStreamingApplication(cfg: ApplicationConfig) {
  def spark_cfg: Map[String, String] = cfg.spark
  def batch_duration: FiniteDuration = cfg.batch_duration
  def checkpoint_dir: String = cfg.checkpoint_dir

  def with_context(scfg: ApplicationConfig, fn: (SparkContext, StreamingContext, DStream[String]) => DStream[String]): Unit = {
    val cfg = new SparkConf()
    spark_cfg.foreach { case (n, v) => cfg.setIfMissing(n, v) }
    val ctx = new SparkContext(cfg)
    val sctx = new StreamingContext(ctx, Seconds(batch_duration.toSeconds))
    val source = KafkaSource(scfg.kafka_source)
    val input = source.create(sctx, scfg.topic_input)

    sctx.checkpoint(checkpoint_dir)

    val output = fn(ctx, sctx, input)

    import KafkaSink._
    output.send(scfg.kafka_sink, scfg.topic_output)

    sctx.start()
    sctx.awaitTermination()
  }
}

import com.typesafe.config.Config

case class ApplicationConfig(
  topic_input: String,
  topic_output: String,
  kafka_source: Map[String, String],
  kafka_sink: Map[String, String],
  spark: Map[String, String],
  batch_duration: FiniteDuration,
  checkpoint_dir: String,
  job: Config
) extends Serializable

object ApplicationConfig {
  import com.typesafe.config.ConfigFactory
  import net.ceedubs.ficus.Ficus._

  def apply(name: String): ApplicationConfig = apply(name, ConfigFactory.load)

  def apply(name: String, all_cfg: Config): ApplicationConfig = {
    val app_cfg = all_cfg.getConfig(s"$name.application")
    new ApplicationConfig(
      app_cfg.as[String]("topics.input"),
      app_cfg.as[String]("topics.output"),
      app_cfg.as[Map[String, String]]("kafka.source"),
      app_cfg.as[Map[String, String]]("kafka.sink"),
      app_cfg.as[Map[String, String]]("spark"),
      app_cfg.as[FiniteDuration]("batch_duration"),
      app_cfg.as[String]("checkpoint_dir"),
      all_cfg.getConfig(s"$name.job")
    )
  }
}

class KafkaSource(cfg: Map[String, String]) {
  def create(ctx: StreamingContext, topic: String): DStream[String] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ctx, cfg, Set(topic)).map(_._2)
  }
}

object KafkaSource {
  def apply(cfg: Map[String, String]): KafkaSource = new KafkaSource(cfg)
}

class KafkaSink(@transient private val st: DStream[String]) extends Serializable {
  private val Producers = mutable.Map[Map[String, Object], KafkaProducer[String, String]]()

  def send(cfg: Map[String, String], topic: String): Unit = {
    st.foreachRDD { rdd =>
      rdd.foreachPartition { recs =>
        val pr = maybeMakeProducer(cfg)
        val tctx = TaskContext.get
        
        val meta = recs.map { rec =>
          // since we sling DStream[String] and Producer[String,
          // String], then our records are Strings
          pr.send(new ProducerRecord(topic, rec))
        }.toList

        meta.foreach { d => d.get() }
      }
    }
  }

  import scala.collection.JavaConverters._

  def maybeMakeProducer(cfg: Map[String, Object]): KafkaProducer[String, String] = {
    val default_cfg = Map(
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val local_cfg = default_cfg ++ cfg

    Producers.getOrElseUpdate(
      local_cfg, {
        val pr = new KafkaProducer[String, String](local_cfg.asJava)
        sys.addShutdownHook {
          pr.close()
        }

        pr
      })
  }
}

object KafkaSink {
  import scala.language.implicitConversions

  implicit def createKafkaSink(st: DStream[String]): KafkaSink = {
    new KafkaSink(st)
  }
}
