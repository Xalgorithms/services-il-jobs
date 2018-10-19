// Copyright (C) 2018 Don Kelly <karfai@gmail.com>
// Copyright (C) 2018 Hayk Pilosyan <hayk.pilos@gmail.com>

// This file is part of Interlibr, a functional component of an
// Internet of Rules (IoR).

// ACKNOWLEDGEMENTS
// Funds: Xalgorithms Foundation
// Collaborators: Don Kelly, Joseph Potvin and Bill Olders.

// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see
// <http://www.gnu.org/licenses/>.
package org.xalgorithms.jobs

import org.xalgorithms.apps._
import org.xalgorithms.bson._

import collection.JavaConverters._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.bson._
import play.api.libs.json._
import play.api.libs.functional.syntax._

class DocumentValue(val doc: BsonDocument, val section: String, val key: String) extends Serializable {
  def find(): Seq[BsonValue] = section match {
    case "envelope" => find_in_envelope(doc, key).filter(_ != null)
    case "items"    => find_in_items(doc, key).filter(_ != null)
  }

  def find_in_envelope(doc: BsonDocument, key: String): Seq[BsonValue] = {
    if (doc.isDocument("envelope")) {
      return Seq(Find(doc.getDocument("envelope"), key))
    }

    Seq()
  }

  def find_in_items(doc: BsonDocument, key: String): Seq[BsonValue] = {
    if (doc.isArray("items")) {
      val items: Seq[BsonValue] = doc.getArray("items").getValues().asScala
      return items
        .filter(_.getBsonType() == BsonType.DOCUMENT)
        .map(v => Find(v.asDocument(), key))
    }

    Seq()
  }
}

object DocumentValue {
  def apply(doc: BsonDocument, section: String, key: String): DocumentValue = {
    new DocumentValue(doc.getDocument("content", new BsonDocument()), section, key)
  }
}

class ApplyOperator(m: Match) extends Serializable {
  def matches_any(actual_values: Seq[BsonValue]): Boolean = {
    actual_values.exists(v => m.match_value(v))
  }
}

object ApplyOperator {
  def apply(op: String, value: String): ApplyOperator = {
    new ApplyOperator(Match(op, value))
  }
}

class ApplicableRules(cfg: ApplicationConfig) extends KafkaSparkStreamingApplication(cfg) {
  implicit val job_name: String = "EffectiveRules"

  def execute(): Unit = {
    with_context(cfg, { (ctx, sctx, events, input) =>
      // 1. build a paired stream from the MongoDB, using "public_id" as the key
      // TODO: this needs to be early combined with the incoming document ids stream to
      // filter it
      val docs_stream = new ConstantInputDStream(sctx, MongoSpark.load[BsonDocument](ctx))
        .map { doc =>
          val public_id = doc.getString("public_id").getValue()
          events.value.info(
            "loading document",
            Map("public_id" -> public_id)
          )

          public_id -> doc
        }
      // => ((public_id), (BsonDocument)

      // 2. From the incoming input stream ("{document_id}:{rule_id}"), build a paired
      // stream using document_id as the key. This will allow a join with the stream of
      // documents coming from MongoDB
      val ids_stream = input.map { s =>
        Json.parse(s)
      }.map { o =>
        val doc_id = (o \ "args" \ "document_id").asOpt[String].getOrElse(null)
        val rule_id = (o \ "args" \ "rule_id").asOpt[String].getOrElse(null)

        events.value.info(
          "extracting ids from incoming JSON",
          Map("document_id" -> doc_id, "rule_id" -> rule_id)
        )
        doc_id -> rule_id
      }
      // => ((document_id), (rule_id))

      // 3. Build a dstream out fo the when keys in Cassandra. This will
      // give us a stream of potential keys to extract from the document.
      // Rows are stored in this table when there are actual rules that
      // have XALGO/WHEN expressions referencing the keys in the rows
      val when_keys_stream = new ConstantInputDStream(
        sctx, sctx.cassandraTable(cfg.cassandra_keyspace, "when_keys")
      )
      // => ((section), (key))

      // 4. Join the stream of documents from Mongo using the
      // document_id to the corresponding rules that came via the
      // input stream, filtering out non-matches (null)
      val combined_rules_stream = ids_stream
        .join(docs_stream)
        .filter(_._2._2 != null)
        .map(tup => Tuple3(tup._1, tup._2._1, tup._2._2))
      // => (document_id, rule_id, Document)

      // 5. Perform a cartesian product between the stream of documents and
      // the stream from the when_keys table. This is used to build a stream
      // of key values from the document. This is a paired stream with "{rule_id}:{section}:{key}
      // used as the key. The values from the Document are captured as a DocumentValue instance
      // that can be used to pull the actual value.
      val doc_values_stream = combined_rules_stream
        .transformWith(when_keys_stream, (_: RDD[(String, String, BsonDocument)]).cartesian(_: RDD[CassandraRow]))
        .map { tup =>
          //tup: ((doc_id, rule_id, doc), (row))
          val section = tup._2.getString("section")
          val key = tup._2.getString("key")

          events.value.info(
            "building document value",
            Map("section" -> section, "key" -> key)
          )
          s"${section}:${key}" -> Tuple3(tup._1._1, tup._1._2, DocumentValue(tup._1._3, section, key))
        }
      // => ((section:key), (doc_id, rule_id, DocumentValue)

      // 6. Build a stream of rows from the whens table in
      // Cassandra. This is the actual match specified in the
      // identified rule. This is a paired stream, keyed on
      // "{section}:{key}". The comparision is retained as an
      // ApplyOperator instance.
      val whens_stream = new ConstantInputDStream(
        sctx, sctx.cassandraTable(cfg.cassandra_keyspace, "whens")
      ).map { cr =>
        val section = cr.getString("section")
        val key = cr.getString("key")
        val rule_id = cr.getString("rule_id")

        events.value.info(
          "building whens maps",
          Map("section" -> section, "key" -> key)
        )
        s"${section}:${key}" -> Tuple2(rule_id, ApplyOperator(cr.getString("op"), cr.getString("val")))
      }
      // => ((section:key), (rule_id, ApplyOperator))

      // 7. Join the stream of values from the documents and the
      // stream of matches from the whens table. Since these streams
      // are joined on "{section}:{key}" - the value we're extracting
      // / testing, we need to further filter the stream to validate
      // that we're working with matching rules. Ultimately, this
      // stream generates the same document_id:rule_id stream
      // filtering out those messages that DID NOT match WHEN
      // conditions.
      doc_values_stream.join(whens_stream)
        // toss away the key we were using for the join, we won't need it
        .map(_._2)
        // filter on matching rule ids -- tup._1 has the rule_id
        // from the input stream; tup._2 from the whens table
        .filter(tup => tup._1._2 == tup._2._1)
        .map { tup =>
          val found = tup._1._3.find()
          val matches_any = tup._2._2.matches_any(found)
          events.value.info(
            "found results in document value",
            Map(
              "section" -> tup._1._3.section,
              "key" -> tup._1._3.key,
              "found" -> found.map { v => v.toString }.mkString("; "),
              "matches_any" -> matches_any.toString,
              "doc" -> tup._1._3.doc.toString
            )
          )
          // for reduceByKey, which isn't working
          //s"${tup._1._1}:${tup._1._2}" -> matches_any
          Tuple3(tup._1._1, tup._1._2, matches_any)
        }
        // .reduceByKey((a: Boolean, b: Boolean) => {
        //   events.value.info(
        //     "reducing repeated keys",
        //     Map("a" -> a.toString, "b" -> b.toString)
        //   )
        //   a && b
        // })
        .filter { tup =>
          events.value.info(
            "filtering stream",
            Map("document_id" -> tup._1, "rule_id" -> tup._2, "match" -> tup._3.toString)
          )
          tup._3
        }
        .map { tup =>
          events.value.gave("delivering", Map("document_id" -> tup._1, "rule_id" -> tup._2))
          Json.obj(
            "context" -> Map("task" -> "compute", "action" -> "applicable"),
            "args" -> Map("document_id" -> tup._1, "rule_id" -> tup._2)
          ).toString          
        }
      // => "document_id:rule_id"
    })
  }
}

object ApplicableRules {
  def execute_job(name: String): Unit = {
    val job = new ApplicableRules(ApplicationConfig(name))
    job.execute()
  }

  def main(args: Array[String]) : Unit = {
    execute_job("ApplicableRules")
  }
}

object ValidateApplicableRules {
  def main(args: Array[String]) : Unit = {
    ApplicableRules.execute_job("ValidateApplicableRules")
  }
}
