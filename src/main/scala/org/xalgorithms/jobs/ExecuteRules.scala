package org.xalgorithms.jobs


import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.xalgorithms.apps._
import org.bson.Document
import org.bson.types.ObjectId
import org.xalgorithms.rule_interpreter.{Context, Steps, interpreter}
import org.xalgorithms.rule_interpreter.utils.{documentToContext, extractRevision, extractSteps}

class ExecuteRules(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg) {
  def extractValues(t: (Array[Document], Array[Document])): (String, String) = {
    var docJson = ""
    var ruleJson = ""

    if (t._1.length > 0) {
      docJson = documentToContext(t._1(0).toJson)
    }
    if (t._2.length > 0) {
      ruleJson = extractSteps(t._2(0).toJson)
    }

    (docJson, ruleJson)
  }

  def applyRulesAndPrepareDocument(d: String, r: String): (Document, Document) = {
    if (d != "" && r != "") {
      val context = new Context(d)
      val steps = new Steps(r)
      val docId = (context.get \ "_id" \ "$oid").get.as[String]

      var records = ""
      val parsedDoc = interpreter.runAll(context, steps, (r: String) => {
        records = r
      })
      val revision = extractRevision(parsedDoc.get)
      val v = Document.parse(revision)

      v.append("_id", new ObjectId())
      v.append("doc_id", docId)

      return (v, Document.parse(records))
    }

    (Document.parse(d), Document.parse(""))
  }

  def execute(): Unit = {
    with_context(cfg, {(ctx, sctx, input) =>
      input.map({id_pair =>
        val parts = id_pair.split(":")
        (parts(0), parts(1))
      }).transform({rdd =>
        val docReadConfig = ReadConfig(Map("collection" -> "documents", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(ctx)))
        val ruleReadConfig = ReadConfig(Map("collection" -> "rules", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(ctx)))

        val count = rdd.count()

        val tuples = rdd.take(count.toInt)

        val res = tuples.map({ids =>
          val document_id = ids._1
          val rule_id = ids._2

          val doc_rd = MongoSpark.load(ctx, docReadConfig)
          val aggregatedDocRdd = doc_rd.withPipeline(Seq(Document.parse("{ $match: { _id : { $oid : '" + document_id + "' } } }")))

          val rule_rd = MongoSpark.load(ctx, ruleReadConfig)
          val aggregatedRuleRdd = rule_rd.withPipeline(Seq(Document.parse("{ $match: { _id : { $oid : '" + rule_id + "' } } }")))

          (aggregatedDocRdd.collect(), aggregatedRuleRdd.collect())
        })

        ctx.parallelize(res)
      })
      .map({r =>
        extractValues(r)
      })
      .map({r =>
        applyRulesAndPrepareDocument(r._1, r._2)
      })
      .transform({rdd =>
        val docs = rdd.map({t =>
          t._1
        })
        val records = rdd.map({t =>
          t._2
        })
        val docsWriteConfig = WriteConfig(Map("collection" -> "revision", "writeConcern.w" -> "majority"), Some(WriteConfig(ctx)))
        docs.saveToMongoDB(docsWriteConfig)

        val recordsWriteConfig = WriteConfig(Map("collection" -> "records", "writeConcern.w" -> "majority"), Some(WriteConfig(ctx)))
        records.saveToMongoDB(recordsWriteConfig)
        docs
      })
      .map({r =>
        r.getObjectId("_id").toString
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
