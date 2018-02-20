package org.xalgorithms.jobs


import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.xalgorithms.apps._
import org.bson.{BsonDocument, Document}
import org.bson.types.ObjectId
import org.xalgorithms.rule_interpreter.{Context, Steps, interpreter}
import org.xalgorithms.rule_interpreter.utils.{documentToContext, extractRevision, extractSteps}

class ExecuteRules(cfg: ApplicationConfig) extends KafkaMongoSparkStreamingApplication(cfg) {
  def extractValues(t: (BsonDocument, BsonDocument)): (String, String) = {
    val docJson = documentToContext(t._1.toJson)
    val ruleJson = extractSteps(t._2.toJson)

    (docJson, ruleJson)
  }

  def applyRulesAndPrepareDocument(d: String, r: String): (String, String) = {
    if (d != "" && r != "") {
      val context = new Context(d)
      val steps = new Steps(r)
      val docId = (context.get \ "_id").get.as[String]

      var records = ""
      val parsedDoc = interpreter.runAll(context, steps, (r: String) => {
        records = r
      })
      val revision = extractRevision(parsedDoc.get)
      val v = Document.parse(revision)
      val id = new ObjectId()

      v.append("public_id", id.toHexString)
      v.append("doc_id", docId)

      return (v.toJson, records)
    }

    (d, "")
  }

  def execute(): Unit = {
    with_context(cfg, {(ctx, sctx, input) =>
      val docReadConfig = ReadConfig(Map("collection" -> "documents", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(ctx)))
      val ruleReadConfig = ReadConfig(Map("collection" -> "rules", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(ctx)))

      val docs_stream = new ConstantInputDStream(sctx, MongoSpark.load[BsonDocument](ctx, docReadConfig))
        .map(doc => doc.getString("public_id").getValue() -> doc)

      val rules_stream = new ConstantInputDStream(sctx, MongoSpark.load[BsonDocument](ctx, ruleReadConfig))
        .map(doc => doc.getString("public_id").getValue() -> doc)

      val ids_stream = input.map { doc_rule_id =>
        val ids = doc_rule_id.split(":")
        ids(0) -> ids(1)
      }
      // => ((document_id), (rule_id))

      val combined_docs_stream = ids_stream.join(docs_stream)
        .filter(_._2._2 != null)
        .map(tup => Tuple2(tup._2._1, tup._1 -> tup._2._2))
      // => (rule_id, (document_id, document))

      val combined_docs_and_rules_stream = combined_docs_stream.join(rules_stream)
        .filter(_._2._2 != null)
        .map(tup => Tuple4(tup._2._1._1, tup._1, tup._2._1._2, tup._2._2))
      // => (document_id, rule_id, document, rule))

      combined_docs_and_rules_stream.map({tup =>
        extractValues(tup._3, tup._4)
      })
      .map({t =>
        applyRulesAndPrepareDocument(t._1, t._2)
      })
      .map({r =>
        r._1
      })
    })
  }
}

object ExecuteRules {
  def main(args: Array[String]) : Unit = {
    val job = new ExecuteRules(ApplicationConfig("ExecuteRules"))
    job.execute()
  }
}
