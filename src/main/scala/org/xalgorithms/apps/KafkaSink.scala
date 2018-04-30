package org.xalgorithms.apps

import org.apache.kafka.clients.producer.KafkaProducer
import scala.collection.JavaConverters._
import scala.collection.mutable

class KafkaSink() extends Serializable {
  private val Producers = mutable.Map[Map[String, Object], KafkaProducer[String, String]]()

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
