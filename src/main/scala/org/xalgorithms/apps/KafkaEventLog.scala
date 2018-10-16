// Copyright (C) 2018 Don Kelly <karfai@gmail.com>
// Copyright (C) 2018 Hayk Pilosyan <hayk.pilos@gmail.com>

// This file is part of Interlibr, a functional component of an
// Internet of Rules (IoR).

// ACKNOWLEDGEMENTS
// Funds: Xalgorithms Foundation
// Collaborators: Don Kelly, Joseph Potvin and Bill Olders.

// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see
// <http://www.gnu.org/licenses/>.
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
    val o = Json.obj(
      "level" -> level,
      "message" -> m,
      "props" -> props
    )
    producer.send(new ProducerRecord("il.audit.compute", o.toString))
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
