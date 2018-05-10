package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements._

class AssembleStep(val name: String, val columns: Seq[Column]) extends Step {
  def execute(ctx: Context) {
    val combined_tbl = columns.foldLeft(Seq[Map[String, IntrinsicValue]]()) { (tbl, col) =>
      col.sources.size match {
        case 0 => tbl
        case _ => combine(tbl, build_table_from_sources(ctx, col.table, col.sources))
      }
    }

    ctx.retain_table("table", name, combined_tbl)
  }

  def combine(
    left: Seq[Map[String, IntrinsicValue]],
    right: Seq[Map[String, IntrinsicValue]]
  ): Seq[Map[String, IntrinsicValue]] = left.size match {
    case 0 => right
    case _ => left.map { r0 => right.map { r1 => r0 ++ r1 } }.flatten
  }

  def merge(
    left: Seq[Map[String, IntrinsicValue]],
    right: Seq[Map[String, IntrinsicValue]]
  ): Seq[Map[String, IntrinsicValue]] = left.size match {
    case 0 => right
    case _ => (left, right).zipped.map { (rl, rr) => rl ++ rr }
  }

  def build_table_from_sources(
    ctx: Context, tr: TableReference, srcs: Seq[TableSource]
  ): Seq[Map[String, IntrinsicValue]] = {
    val src_tbl = ctx.lookup_table(tr.section, tr.name)
    srcs.foldLeft(Seq[Map[String, IntrinsicValue]]()) { (tbl, src) =>
      // sources are within the SAME table and are the same size, so we zip/merge them together
      // as if this were an append
      merge(tbl, src match {
        case (cols: ColumnsTableSource) => src_tbl.map { r =>
          r.filterKeys { k => cols.columns.length == 0 || cols.columns.contains(k) }
        }
        case (col: ColumnTableSource) => src_tbl.map { r =>
          Map(col.name -> r(col.source))
        }
        case _ => Seq[Map[String, IntrinsicValue]]()
      })
    }
  }
}
