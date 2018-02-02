package org.xalgorithms.rule_interpreter

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import play.api.libs.json.{JsNull, Json}

class commonTest extends FunSuite with BeforeAndAfterEach {
  override def beforeEach() {

  }

  override def afterEach() {

  }

  test("Set nested value properly when all keys are present") {
    val input = Json.obj(
      "x" -> Json.obj(
        "y" -> Json.obj(
          "z" -> "value"
        )
      )
    )
    val keys = "x.y.z"
    val newValue = "new value"
    val expect = Json.obj(
      "x" -> Json.obj(
        "y" -> Json.obj(
          "z" -> "new value"
        )
      )
    )
    val actual = common.recursiveSetKeys(input, keys, newValue)

    assert(actual == expect)
  }

  test("Set nested value properly when a key is null") {
    val input = Json.obj(
      "x" -> Json.obj(
        "y" -> JsNull
      )
    )

    val keys = "x.y.z"
    val newValue = "new value"
    val expect = Json.obj(
      "x" -> Json.obj(
        "y" -> Json.obj(
          "z" -> "new value"
        )
      )
    )
    val actual = common.recursiveSetKeys(input, keys, newValue)

    assert(actual == expect)
  }

  test("Set nested value properly when a key doesn't exist") {
    val input = Json.obj(
      "x" -> Json.obj()
    )

    val keys = "x.y.z"
    val newValue = "new value"
    val expect = Json.obj(
      "x" -> Json.obj(
        "y" -> Json.obj(
          "z" -> "new value"
        )
      )
    )
    val actual = common.recursiveSetKeys(input, keys, newValue)

    assert(actual == expect)
  }

  test("Get nested value properly by dot separated key string") {
    val input = Json.obj(
      "x" -> Json.obj(
        "y" -> Json.obj(
          "z" -> "value"
        )
      )
    )

    val keys = "x.y.z"
    val expect = "value"
    val actual = common.getValueByKeyString(input, keys)

    assert(actual == expect)
  }

  test("Get null when key doesn't exist") {
    val input = Json.obj(
      "x" -> Json.obj(
        "y" -> Json.obj()
      )
    )

    val keys = "x.y.z"
    val expect = null
    val actual = common.getValueByKeyString(input, keys)

    assert(actual == expect)
  }
}
