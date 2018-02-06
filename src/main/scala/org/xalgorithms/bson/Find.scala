package org.xalgorithms.bson

import org.bson._

object Find {
  def apply(doc: BsonDocument, path: Array[String]): BsonValue = {
    if (path.length == 1) {
      return doc.get(path.head)
    }

    val ch_doc = doc.getDocument(path.head)
    if (null == ch_doc) {
      return null
    }

    return apply(ch_doc, path.tail)
  }

  def apply(doc: BsonDocument, path: String): BsonValue = {
    apply(doc, path.split('.'))
  }
}
