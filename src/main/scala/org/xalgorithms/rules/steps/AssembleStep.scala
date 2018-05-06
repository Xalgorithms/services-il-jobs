package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements.{ Column, ColumnsTableSource, Value }

class AssembleStep(val name: String, val columns: Seq[Column]) extends Step {
  def execute(ctx: Context) {
    val combined_tbl = columns.foldLeft(Seq[Map[String, Value]]()) { (tbl, col) =>
      val src = col.sources.head
      var stbl = Seq[Map[String, Value]]()

      if (col.sources.length > 1) {
        println("DEBT: only single column sources are supported")
      }

      val cols_source = src.asInstanceOf[ColumnsTableSource]
      if (null != cols_source) {
        stbl = ctx.lookup_table(col.table.section, col.table.name).map { o =>
          o.filterKeys { k =>
            cols_source.columns.length == 0 || cols_source.columns.contains(k)
          }
        }

      } else {
        println("DEBT: only ColumnsTableSource is supported")
      }

      if (tbl.length == 0) stbl else tbl.map { r0 => stbl.map { r1 => r0 ++ r1 } }.flatten
    }

    ctx.retain_table("table", name, combined_tbl)
  }
}
