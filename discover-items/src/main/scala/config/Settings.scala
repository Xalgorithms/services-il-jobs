package config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  lazy val brokers = String.join(";", config.getStringList("brokers"))
  lazy val receiver_topic =  config.getString("receiver_topic")
  lazy val producer_topic =  config.getString("producer_topic")
  lazy val cassandra_host =  config.getString("cassandra_host")
  lazy val arango_host =  config.getString("arango_host")
  lazy val arango_username =  config.getString("arango_username")
  lazy val arango_password =  config.getString("arango_password")
}
