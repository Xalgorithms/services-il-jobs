package org.xalgorithms.rules.steps

import org.xalgorithms.rules.{ Context }
import org.xalgorithms.rules.elements._

class MapStep(table: TableReference, assignments: Seq[Assignment]) extends AssignmentStep(table, assignments) {
  def execute(ctx: Context) {
    val tbl = ctx.lookup_table(table.section, table.name)
    ctx.retain_table(table.section, table.name, tbl.map { row =>
      row ++ assignments.foldLeft(Map[String, IntrinsicValue]()) { (o, ass) =>
        val aov = resolve_to_atomic_value(row, ass.source)

        aov match {
          case Some(av) => o ++ Map(ass.target -> av)
          case None => o
        }
      }
    })
  }

  def resolve_to_atomic_value(row: Map[String, IntrinsicValue], v: Value): Option[IntrinsicValue] = v match {
    case (rv: ReferenceValue) => if (rv.section == "_context") Some(row.getOrElse(rv.key, null)) else None
    case (fv: FunctionValue) => apply(fv.name, fv.args.map(arg => resolve_to_atomic_value(row, arg)))
    case (iv: IntrinsicValue) => Some(iv)
    case _ => None
  }

  def apply(fn: String, args: Seq[Option[IntrinsicValue]]): Option[IntrinsicValue] = fn match {
    case "add" => Some(apply_add(args))
    case _ => None
  }

  def apply_add(args: Seq[Option[IntrinsicValue]]): IntrinsicValue = {
    new NumberValue(args.foldLeft(BigDecimal(0.0)) { (sum, n) =>
      n match {
        case Some(v) => sum + make_number(v)
        case None => sum
      }
    })
  }

  def make_number(v: Value): BigDecimal = v match {
    case (nv: NumberValue) => nv.value
    case (sv: StringValue) => BigDecimal(sv.value)
    case _ => 0.0
  }
}

