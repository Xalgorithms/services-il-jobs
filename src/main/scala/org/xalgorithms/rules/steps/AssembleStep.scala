package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements.{ Column }

class AssembleStep(val name: String, val columns: Seq[Column]) extends Step {
  def execute(ctx: Context) {
  }
}

