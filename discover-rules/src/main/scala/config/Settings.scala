package config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  lazy val brokers = String.join(";", config.getStringList("brokers"))
  lazy val topic =  config.getString("topic")
}
