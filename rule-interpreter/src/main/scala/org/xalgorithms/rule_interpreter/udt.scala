package org.xalgorithms.rule_interpreter

import play.api.libs.json.Json

object udt {
  sealed trait BaseUDT

  case class Amount(value: BigDecimal, currency_code: String) extends BaseUDT
  case class Measure(value: BigDecimal, unit: String) extends BaseUDT
  case class Pricing(orderable_factor: BigDecimal, price: Option[Amount], quantity: Measure) extends BaseUDT
  case class TaxComponent(amount: Amount, taxable: Amount) extends BaseUDT
  case class ItemTax(total: Amount, components: List[TaxComponent]) extends BaseUDT
  case class Item(id: String, price: Option[Amount], quantity: Measure, pricing: Pricing, tax: String) extends BaseUDT
  case class Revision(id: String, Array: List[Item]) extends BaseUDT

  case class Period(timezone: Option[String], starts: Option[String], ends: Option[String]) extends BaseUDT
  case class Envelope(issued: Option[String], country: Option[String], region: Option[String], party: Option[String], period: Option[Period]) extends BaseUDT
  case class Invoice(_id: String, items: Array[Item], envelope: Envelope) extends BaseUDT

  case class Operand(`type`: String, value: String) extends BaseUDT
  case class RuleExpression(left: Operand, right: Operand, op: String) extends BaseUDT
  case class RuleExpressionWrapper(expr: RuleExpression) extends BaseUDT
  case class WhenRule(envelope: Option[Array[RuleExpressionWrapper]], item: Option[Array[RuleExpressionWrapper]]) extends BaseUDT
  case class RuleItem(whens: WhenRule) extends BaseUDT

  case class Rule(_id: String, rule_id: String, items: Array[RuleItem]) extends BaseUDT

  case class Table(`type`: String, section: String, key: String)
  case class Assignment(column: String, `type`: String, name: Option[String], section: Option[String], key: Option[String], value: Option[String], args: Option[Array[Assignment]])
  case class Step(name: String, table: Table, assignments: Array[Assignment])

  implicit val tableReads = Json.reads[Table]
  implicit val assignmentReads = Json.reads[Assignment]
  implicit val stepReads = Json.reads[Step]
}