package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ LoadTableSource }
import org.xalgorithms.rules.elements.{ PackagedTableReference }

import play.api.libs.json._
import scala.io.Source

class ResourceLoadTableSource extends LoadTableSource {
  def read(ptref: PackagedTableReference): JsValue = {
    return Json.parse(Source.fromURL(getClass.getResource(s"/${ptref.name}.json")).mkString)
  }
}
