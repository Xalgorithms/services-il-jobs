package config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  lazy val brokers = String.join(";", config.getStringList("brokers"))
  lazy val receiver_topic =  config.getString("receiver_topic")
  lazy val producer_topic =  config.getString("producer_topic")
  lazy val cassandra_host =  config.getString("cassandra_host")
}
