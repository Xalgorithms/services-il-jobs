package org.xalgorithms.apps

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.libs.json._
import scala.collection.JavaConverters._

class KafkaEventLog(producer_fn: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer = producer_fn()

  def error(m: String, props: Map[String, String] = Map()) {
    send("error", m, props)
  }

  def warn(m: String, props: Map[String, String] = Map()) {
    send("warn", m, props)
  }

  def info(m: String, props: Map[String, String] = Map()) {
    send("info", m, props)
  }

  def gave(m: String, props: Map[String, String] = Map()) {
    send("gave", m, props)
  }

  def got(m: String, props: Map[String, String] = Map()) {
    send("got", m, props)
  }

  def send(level: String, m: String, props: Map[String, String] = Map()) {
    producer.send(new ProducerRecord("xadf.log.events", Json.toJson(props ++ Map("level" -> level, "message" -> m)).toString))
  }
}

object KafkaEventLog {
  def apply(cfg: Map[String, Object]): KafkaEventLog = {
    val producer_fn = () => {
      val default_cfg = Map(
        "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
      )
      val local_cfg = default_cfg ++ cfg
      val pr = new KafkaProducer[String, String](local_cfg.asJava)
      sys.addShutdownHook { pr.close() }

      pr
    }

    new KafkaEventLog(producer_fn)
  }
}
