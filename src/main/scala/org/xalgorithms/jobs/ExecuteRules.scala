package org.xalgorithms.jobs


import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.xalgorithms.apps._
import org.bson.{BsonDocument, BsonString}
import org.bson.types.ObjectId
import org.xalgorithms.rule_interpreter.{Context, Steps, interpreter}
import org.xalgorithms.rule_interpreter.utils.{documentToContext, extractRevision, extractSteps}


class ExecuteRules(cfg: ApplicationConfig) extends KafkaMongoSparkStreamingApplication(cfg) {
  def extractValues(t: (BsonDocument, BsonDocument)): (String, String) = {
    val docJson = documentToContext(t._1.toJson)
    val ruleJson = extractSteps(t._2.toJson)

    (docJson, ruleJson)
  }

  def applyRulesAndPrepareDocument(d: String, r: String, tables: List[(String, String)]): (String, String) = {
    if (d != "" && r != "") {
      val context = new Context(d, tables)
      val steps = new Steps(r)
      val docId = (context.get \ "public_id").get.as[String]

      val result = interpreter.runAll(context, steps, true)
      val revision = extractRevision(result._1.get)
      val records = result._2

      val v = BsonDocument.parse(revision)
      val id = new ObjectId()
      v.append("public_id", new BsonString(id.toHexString))
      v.append("doc_id", new BsonString(docId))

      return (v.toJson, records)
    }

    (d, "")
  }

  def extract_require_ids(id: String, rule: BsonDocument): Array[(String, (String, BsonDocument))] = {
    val requires = rule.get("requires")

    if (requires == null || !requires.isArray) {
      return Array(("", (id, rule)))
    }
    rule.get("requires").asArray.toArray.map { r =>
      val table_id = r.asInstanceOf[BsonDocument].get("id").asString().getValue
      (table_id, (id, rule))
    }
  }

  def execute(): Unit = {
    with_context(cfg, {(ctx, sctx, input) =>
      val docReadConfig = ReadConfig(Map("collection" -> "documents", "readPreference.name" -> "secondaryPreferred"))
      val ruleReadConfig = ReadConfig(Map("collection" -> "rules", "readPreference.name" -> "secondaryPreferred"))
      val tablesReadConfig = ReadConfig(Map("collection" -> "tables", "readPreference.name" -> "secondaryPreferred"))

      val docs_stream = new ConstantInputDStream(sctx, MongoSpark.load[BsonDocument](ctx, docReadConfig))
        .map(doc => doc.getString("public_id").getValue() -> doc)

      val rules_stream = new ConstantInputDStream(sctx, MongoSpark.load[BsonDocument](ctx, ruleReadConfig))
        .map(rule => rule.getString("public_id").getValue() -> rule)

      val tables_stream = new ConstantInputDStream(sctx, MongoSpark.load[BsonDocument](ctx, tablesReadConfig))
        .map(table => table.getString("public_id").getValue() -> table.toJson)

      val ids_stream = input.map { doc_rule_id =>
        val ids = doc_rule_id.split(":")
        ids(0) -> ids(1)
      }
      // => ((document_id), (rule_id))

      val combined_docs_stream = ids_stream.join(docs_stream)
        .filter(_._2._2 != null)
        .map(tup => Tuple2(tup._2._1, tup._1 -> tup._2._2))
      // => (rule_id, (document_id, document))

      val combined_rules_and_tables_stream = rules_stream
        .flatMap({tup =>
          extract_require_ids(tup._1, tup._2)
        })
        // => (table_id, (rule_id, rule))
        .leftOuterJoin(tables_stream)
        // => (table_id, ((rule_id, rule), Option[table]))
        .map(tup => ((tup._2._1._1, tup._2._1._2), List( (tup._1, tup._2._2.getOrElse("")) )))
        // => ((rule_id, rule), (table_id, table))
        .reduceByKey({(s1: List[(String, String)], s2: List[(String, String)]) => s1 ::: s2}, 3)
        // => ((rule_id, rule), Seq(table_id, table))
        .map(tup => Tuple2(tup._1._1, (tup._1._2, tup._2)))
        // => (rule_id, (rule, Seq(table_id, table)))


      val combined_docs_and_rules_stream = combined_docs_stream.join(combined_rules_and_tables_stream)
        .filter(_._2._2 != null)
        //.map(tup => Tuple4(tup._2._1._1, tup._1, tup._2._1._2, tup._2._2))
        .map(tup => Tuple5(tup._2._1._1, tup._1, tup._2._1._2, tup._2._2._1, tup._2._2._2))
        // => (document_id, rule_id, document, rule, tables)

      combined_docs_and_rules_stream
        .map({tup =>
          val (doc, rule) = extractValues(tup._3, tup._4)
          (doc, rule, tup._5)
        })
        .map({t =>
          applyRulesAndPrepareDocument(t._1, t._2, t._3)
        })
        .map({r =>
          r
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
