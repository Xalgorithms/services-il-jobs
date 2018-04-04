package org.xalgorithms.rules.steps

import org.xalgorithms.rules.elements.{ PackagedTableReference }

class RequireStep(val table_reference: PackagedTableReference, val indexes: Seq[String]) extends Step {
}

