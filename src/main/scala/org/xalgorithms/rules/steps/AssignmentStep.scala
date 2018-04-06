package org.xalgorithms.rules.steps

import org.xalgorithms.rules.elements.{ Assignment, Reference }

abstract class AssignmentStep(val table: Reference, val assignments: Seq[Assignment]) extends Step {
}

