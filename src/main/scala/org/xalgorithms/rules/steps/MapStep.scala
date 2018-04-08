package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements.{ Assignment, Reference, Value }

class MapStep(table: Reference, assignments: Seq[Assignment]) extends AssignmentStep(table, assignments) {
  def execute(ctx: Context) {
    val tbl = ctx.find_in_section(table.section, table.key)
    ctx.retain(table.section, table.key, tbl.map { r =>
      r ++ assignments.foldLeft(Map[String, Value]()) { (o, ass) =>
        val rs = ass.source.asInstanceOf[Reference]

        if (rs.section == "_context") {
          o ++ Map(ass.target -> r.getOrElse(rs.key, null))
        } else {
          println("DEBT: only context sources are supported")
          o
        }
      }
    })
  }
}

