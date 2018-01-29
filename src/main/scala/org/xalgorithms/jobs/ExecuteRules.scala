package org.xalgorithms.jobs


import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.xalgorithms.apps._
import org.bson.Document
import org.bson.types.ObjectId
import org.xalgorithms.rule_interpreter.interpreter
import org.xalgorithms.rule_interpreter.utils.{documentToContext, extractStep, extractRevision}

class ExecuteRules(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg) {
  def extractValues(t: (Array[Document], Array[Document])): (String, String) = {
    var docJson = ""
    var ruleJson = ""

    if (t._1.length > 0) {
      docJson = documentToContext(t._1(0).toJson)
    }
    if (t._2.length > 0) {
      ruleJson = extractStep(t._2(0).toJson)
    }

    (docJson, ruleJson)
  }

  def applyRules(d: String, r: String): String = {
    if (d != "" && r != "") {
      val parsedDoc = interpreter.parse(d, r)
      return extractRevision(parsedDoc)
    }
    ""
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
        applyRules(r._1, r._2)
      })
      .map({doc =>
        val v = Document.parse(doc)
        v.append("_id", new ObjectId())
        v
      })
      .transform({r =>
        val writeConfig = WriteConfig(Map("collection" -> "revision", "writeConcern.w" -> "majority"), Some(WriteConfig(ctx)))
        r.saveToMongoDB(writeConfig)
        r
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
