package org.xalgorithms.rules.elements

class Value {
}

class Reference(val section: String, val key: String) extends Value {
}

class NumberValue(val value: BigDecimal) extends Value {
}

class StringValue(val value: String) extends Value {
}

class FunctionValue(val name: String, val args: Seq[Value]) extends Value {
}
