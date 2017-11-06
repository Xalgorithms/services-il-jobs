package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}


object SparkUtils {
  var isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }
  def getSparkContext(appName: String) = {
    val conf = new SparkConf()
        .setAppName(appName)

    if (isIDE) {
      conf.setMaster("local[*]")
    }

    val sc = SparkContext.getOrCreate(conf)
    sc
  }
}
