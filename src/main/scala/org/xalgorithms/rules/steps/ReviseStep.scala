package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements.{ Reference, Revision }

class ReviseStep(val table: Reference, val revisions: Seq[Revision]) extends Step {
  def execute(ctx: Context) {
  }
}

