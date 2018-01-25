package org.xalgorithms.rule_interpreter


import org.scalatest.{BeforeAndAfterEach, FunSuite}
import play.api.libs.json.Json

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

    val expected$ = Json.obj(
      "a" -> "text 1"
    )

    assert(actual$ == expected$)
  }

  test("Should interpret map2 properly") {
    val contextSource = Source.fromURL(getClass.getResource("/context.json"))
    val context = contextSource.mkString

    val stepsSource = Source.fromURL(getClass.getResource("/map2.json"))
    val step = stepsSource.mkString

    val actualContext = interpreter.parse(context, step)
    val actual$ = (actualContext \ "$").get

    val expected$ = Json.obj(
      "a" ->"text 1",
      "b" -> "text 2",
      "c" -> 2,
      "d" -> "5"
    )

    assert(actual$ == expected$)
  }

  test("Should interpret map3 properly") {
    val contextSource = Source.fromURL(getClass.getResource("/context.json"))
    val context = contextSource.mkString

    val stepsSource = Source.fromURL(getClass.getResource("/map3.json"))
    val step = stepsSource.mkString

    val actualContext = interpreter.parse(context, step)
    val actual$ = (actualContext \ "$").get

    val expected$ = Json.obj(
      "b" -> "1",
      "a" -> 12
    )

    assert(actual$ == expected$)
  }

  test("Should interpret revise1 properly") {
    val contextSource = Source.fromURL(getClass.getResource("/context.json"))
    val context = contextSource.mkString

    val stepsSource = Source.fromURL(getClass.getResource("/revise1.json"))
    val step = stepsSource.mkString

    val actualContext = interpreter.parse(context, step)
    val actual$ = (actualContext \ "revision").get

    val expected$ = Json.obj(
      "x" -> Json.obj(
        "y" -> Json.obj(
          "z" -> "text 1",
          "w" -> "1"
        )
      ),
      "p" -> Json.obj(
        "q" -> "text 2"
      ),
      "a" -> "text 1",
      "b" -> "text 2",
      "c" -> 2,
      "d" -> "s"
    )

    assert(actual$ == expected$)
  }

  test("Should interpret revise2 properly") {
    val contextSource = Source.fromURL(getClass.getResource("/context.json"))
    val context = contextSource.mkString

    val stepsSource = Source.fromURL(getClass.getResource("/revise2.json"))
    val step = stepsSource.mkString

    val actualContext = interpreter.parse(context, step)
    val actual$ = (actualContext \ "revision").get

    val expected$ = Json.obj(
      "x" -> Json.obj(
        "y" -> Json.obj(
          "z" -> "text 1",
          "w" -> "1"
        )
      ),
      "p" -> Json.obj(
        "q" -> "text 2"
      ),
      "a" -> 6
    )

    assert(actual$ == expected$)
  }

  test("Should interpret assemble1 properly") {
    val contextSource = Source.fromURL(getClass.getResource("/context2.json"))
    val context = contextSource.mkString

    val stepsSource = Source.fromURL(getClass.getResource("/assemble1.json"))
    val step = stepsSource.mkString

    val actualContext = interpreter.parse(context, step)
    val actual$ = (actualContext \ "table").get

    val expected$ = Json.obj(
      "table1" -> Json.obj(
        "a" -> Json.obj(
          "b" -> "5"
        ),
        "c0" -> "7"
      ),
      "table0" -> Json.obj(
        "c0" -> "7"
      )
    )

    assert(actual$ == expected$)
  }

  test("Should interpret assemble2 properly") {
    val contextSource = Source.fromURL(getClass.getResource("/context2.json"))
    val context = contextSource.mkString

    val stepsSource = Source.fromURL(getClass.getResource("/assemble2.json"))
    val step = stepsSource.mkString

    val actualContext = interpreter.parse(context, step)
    val actual$ = (actualContext \ "table").get

    val expected$ = Json.obj(
      "table1" -> Json.obj(
        "a" -> Json.obj(
          "b" -> "5"
        ),
        "c0" -> "7"
      ),
      "table0" -> Json.obj(
        "nc0" -> "7"
      )
    )

    assert(actual$ == expected$)
  }

  test("Should interpret assemble3 properly") {
    val contextSource = Source.fromURL(getClass.getResource("/context3.json"))
    val context = contextSource.mkString

    val stepsSource = Source.fromURL(getClass.getResource("/assemble3.json"))
    val step = stepsSource.mkString

    val actualContext = interpreter.parse(context, step)
    val actual$ = (actualContext \ "table").get

    val expected$ = Json.obj(
      "table1" -> Json.obj(
        "a" -> Json.obj(
          "b" -> "5"
        ),
        "c0" -> "7",
        "c1" -> "1"
      ),
      "table2" -> Json.obj(
        "b0" -> "8"
      ),
      "table0" -> Json.obj(
        "nc0" -> "7",
        "nc1" -> "1",
        "nc2" -> "8"
      )
    )

    assert(actual$ == expected$)
  }
}
