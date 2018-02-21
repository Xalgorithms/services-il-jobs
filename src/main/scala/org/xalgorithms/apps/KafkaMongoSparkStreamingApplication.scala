//package org.xalgorithms.apps
//
//import java.lang.management.ManagementFactory
//
//import com.mongodb.client.MongoCollection
//import com.mongodb.spark.MongoConnector
//import com.mongodb.spark.config.WriteConfig
//import org.apache.kafka.clients.producer.ProducerRecord
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.dstream.DStream
//import org.bson.Document
//import scala.collection.JavaConverters._
//
//
//class KafkaMongoSparkStreamingApplication(cfg: ApplicationConfig) extends KafkaSparkStreamingApplication(cfg) {
//  override def with_context(scfg: ApplicationConfig, fn: (SparkContext, StreamingContext, DStream[String]) => DStream[String]): Unit = {
//    val isIDE = {
//      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
//    }
//    val cfg = new SparkConf()
//    spark_cfg.foreach { case (n, v) => cfg.setIfMissing(n, v) }
//
//    if (isIDE) {
//      cfg.setMaster("local[*]")
//    }
//
//    val ctx = new SparkContext(cfg)
//    val sctx = new StreamingContext(ctx, Seconds(batch_duration.toSeconds))
//    val source = KafkaSource(scfg.kafka_source)
//    val input = source.create(sctx, scfg.topic_input)
//
//    // FIXME: Spits out errors, when spark context is accessed from transform
//    // sctx.checkpoint(checkpoint_dir)
//
//    val output = fn(ctx, sctx, input)
//
//    import KafkaMongoSink._
//    output.send(scfg.kafka_sink, scfg.topic_output, ctx)
//
//    sctx.start()
//    sctx.awaitTermination()
//  }
//}
//
//class KafkaMongoSink(@transient private val st: DStream[String]) extends KafkaSink(st) {
//  def send(cfg: Map[String, String], topic: String, ctx: SparkContext): Unit = {
//    val writeConfig = WriteConfig(Map("collection" -> "revision", "writeConcern.w" -> "majority", "replaceDocument" -> "false"), Some(WriteConfig(ctx)))
//    val mongoConnector = MongoConnector(writeConfig.asOptions)
//
//    st.foreachRDD { rdd =>
//      rdd.foreachPartition (recs => if(recs.nonEmpty) {
//        val pr = maybeMakeProducer(cfg)
//
//        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
//          recs.grouped(writeConfig.maxBatchSize).foreach({ batch =>
//            collection.insertMany(batch.toList.map({rec =>
//              val doc = Document.parse(rec)
//              pr.send(new ProducerRecord(topic, doc.getString("public_id")))
//              doc
//            }).asJava)
//          })
//        })
//      })
//    }
//  }
//}
//
//object KafkaMongoSink {
//  import scala.language.implicitConversions
//
//  implicit def createKafkaMongoSink(st: DStream[String]): KafkaMongoSink = {
//    new KafkaMongoSink(st)
//  }
//}
