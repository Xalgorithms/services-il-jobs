package org.xalgorithms.rule_interpreter


import org.scalatest.{BeforeAndAfterEach, FunSuite}
import play.api.libs.json.{JsNumber, JsObject, JsString}

import scala.io.Source

class interpreterTest extends FunSuite with BeforeAndAfterEach {

  override def beforeEach() {

  }

  override def afterEach() {

  }

  test("Should interpret map1 properly") {
    val contextSource = Source.fromURL(getClass.getResource("/context.json"))
    val context = contextSource.mkString

    val stepsSource = Source.fromURL(getClass.getResource("/map1.json"))
    val step = stepsSource.mkString

    val actualContext = interpreter.parse(context, step)
    val actual$ = (actualContext \ "$").get

    val expected$ = JsObject(Seq("a" -> JsString("text 1")))

    assert(actual$ == expected$)
  }

  test("Should interpret map2 properly") {
    val contextSource = Source.fromURL(getClass.getResource("/context.json"))
    val context = contextSource.mkString

    val stepsSource = Source.fromURL(getClass.getResource("/map2.json"))
    val step = stepsSource.mkString

    val actualContext = interpreter.parse(context, step)
    val actual$ = (actualContext \ "$").get

    val expected$ = JsObject(Seq("a" -> JsString("text 1"), "b" -> JsString("text 2"), "c" -> JsNumber(2), "d" -> JsString("5")))

    assert(actual$ == expected$)
  }
}
