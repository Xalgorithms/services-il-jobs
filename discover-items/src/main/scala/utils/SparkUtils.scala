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
        .set("spark.mongodb.input.uri", Settings.mongo_host)
        .set("spark.mongodb.output.uri", Settings.mongo_host)

    if (isIDE) {
      conf.setMaster("local[*]")
    }

    val sc = SparkContext.getOrCreate(conf)
    sc
  }
}
