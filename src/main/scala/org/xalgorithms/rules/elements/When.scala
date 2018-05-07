package org.xalgorithms.rules.elements

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements._

class When(val left: Value, val right: Value, val op: String) {
  def evaluate(ctx: Context): Boolean = {
    ResolveValue(left, ctx).matches(ResolveValue(right, ctx), op)
  }
}
