package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements.{ Assignment, Reference, Value }

class MapStep(table: Reference, assignments: Seq[Assignment]) extends AssignmentStep(table, assignments) {
  def execute(ctx: Context) {
    val tbl = ctx.find_in_section(table.section, table.key)
    ctx.retain(table.section, table.key, tbl.map { row =>
      row ++ assignments.foldLeft(Map[String, Value]()) { (o, ass) =>
        val av = resolve_assignment_source(row, ass.source)

        if (null != av) {
          o ++ Map(ass.target -> av)
        } else {
          println("DEBT: unsupported or unknown source")
          o
        }
      }
    })
  }

  def resolve_assignment_source(row: Map[String, Value], v: Value): Value = v match {
    case (rv: Reference) => if (rv.section == "_context") row.getOrElse(rv.key, null) else null
    case _ => null
  }
}

