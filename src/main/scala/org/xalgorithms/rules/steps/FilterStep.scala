package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements.{ Reference, TableReference, When }

class FilterStep(val table: TableReference, val filters: Seq[When]) extends Step {
  def execute(ctx: Context) {
    val tbl = ctx.lookup_table(table.section, table.key)

    ctx.retain_table(table.section, table.key, tbl.filter { r =>
      filters.foldLeft(false) { (v, wh) =>
        val lr = wh.left.asInstanceOf[Reference]
        val rr = wh.right.asInstanceOf[Reference]

        if (null != lr && lr.section == "_context" && null != rr && rr.section == "_context") {
          val lv = r.getOrElse(lr.key, null)
          if (null != lv) {
            lv.matches(r.getOrElse(rr.key, null), wh.op)
          } else {
            println("WARN: left value is not in the table")
            false
          }
        } else {
          println("DEBT: only contextual reference filters are supported")
          false
        }
      }
    })
  }
}

