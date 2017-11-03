package org.xalgorithms.discover_rules;

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Job {
  def main(args: Array[String]) {
    val sp_conf = new SparkConf().setAppName("DiscoverRulesJob")
    val ssc = new StreamingContext(sp_conf, Seconds(1))

    // TODO: connect to a stream, etc

    ssc.start();
    ssc.awaitTermination();
  }
}
