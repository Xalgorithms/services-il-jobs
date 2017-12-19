package utils

import java.lang.management.ManagementFactory

import config.Settings
import org.apache.spark.{SparkConf, SparkContext}


object SparkUtils {
  var isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }
  def getSparkContext(appName: String) = {
    val conf = new SparkConf()
        .setAppName(appName)
        .set("spark.cassandra.connection.host", Settings.cassandra_host)
        .set("arangodb.hosts", Settings.arango_host)
        .set("arangodb.user", Settings.arango_username)
        .set("arangodb.password", Settings.arango_password)

    if (isIDE) {
      conf.setMaster("local[*]")
    }

    val sc = SparkContext.getOrCreate(conf)
    sc
  }
}
