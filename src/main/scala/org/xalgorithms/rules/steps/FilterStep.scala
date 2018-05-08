package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context, RowContext }
import org.xalgorithms.rules.elements.{ ReferenceValue, TableReference, When }

class FilterStep(val table: TableReference, val filters: Seq[When]) extends Step {
  def execute(ctx: Context) {
    val tbl = ctx.lookup_table(table.section, table.name)

    ctx.retain_table(table.section, table.name, tbl.filter { r =>
      val rctx = new RowContext(ctx, Map(), r)
      filters.foldLeft(true) { (v, wh) => v && wh.evaluate(rctx) }
    })
  }
}

