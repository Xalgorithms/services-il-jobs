package org.xalgorithms.bson

import org.bson._

object Find {
  def apply(doc: BsonDocument, path: Array[String]): BsonValue = {
    if (path.length == 1) {
      return doc.get(path.head)
    }

    if (doc.isDocument(path.head)) {
      return apply(doc.getDocument(path.head), path.tail)
    }

    null
  }

  def apply(doc: BsonDocument, path: String): BsonValue = {
    apply(doc, path.split('.'))
  }
}
