package org.xalgorithms.rule_interpreter

import play.api.libs.json.{JsValue, Json}

import scala.io.Source

object testHelper {
  def load(path: String, cb: (Context, Steps, JsValue) => Unit) = {
    val source = Source.fromURL(getClass.getResource(s"/$path.json"))
    val json = Json.parse(source.mkString).as[List[JsValue]]

    json.foreach({j =>
      val contextPath = (j \ "context").get.as[String]
      val inputStr = Json.stringify((j \ "input" \ "steps").get)
      val expected = (j \ "expects").get

      val context = loadContext(contextPath)
      val input = new Steps(inputStr)

      cb(context, input, expected)
    })
  }

  private[this] def loadContext(path: String): Context = {
    val c = Source.fromURL(getClass.getResource(s"/contexts/$path.json"))
    new Context(c.mkString)
  }
}
