package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements.{ Reference, RevisionSource }

class ReviseStep(val table: Reference, val revisions: Seq[RevisionSource]) extends Step {
  def execute(ctx: Context) {
  }
}

