package org.xalgorithms.rule_interpreter


import org.scalatest.{BeforeAndAfterEach, FunSuite}

class interpreterTest extends FunSuite with BeforeAndAfterEach {

  override def beforeEach() {

  }

  override def afterEach() {

  }

  test("Should interpret map properly") {
    testHelper.load("map", (c, steps, expected) =>{
      val actualContext = interpreter.runAll(c, steps)._1.get
      val actual$ = (actualContext \ "$").get

      assert(actual$ == expected)
    })
  }

  test("Should interpret revise properly") {
    testHelper.load("revise", (c, steps, expected) =>{
      val actualContext = interpreter.runAll(c, steps)._1.get
      val actual$ = (actualContext \ "revision").get

      assert(actual$ == expected)
    })
  }

  test("Should interpret assemble properly") {
    testHelper.load("assemble", (c, steps, expected) =>{
      val actualContext = interpreter.runAll(c, steps)._1.get
      val actual$ = (actualContext \ "table").get

      assert(actual$ == expected)
    })
  }

  test("Should interpret a sequence of map and revise properly") {
    testHelper.load("map_and_revise", (c, steps, expected) =>{
      val actualContext = interpreter.runAll(c, steps)._1.get
      val actual$ = (actualContext \ "revision").get

      assert(actual$ == expected)
    })
  }
}
