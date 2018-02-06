package org.xalgorithms.apply_rules

object udt {
  sealed trait BaseUDT

  case class Amount(value: BigDecimal, currency_code: String) extends BaseUDT
  case class Measure(value: BigDecimal, unit: String) extends BaseUDT
  case class Pricing(orderable_factor: BigDecimal, price: Option[Amount], quantity: Measure) extends BaseUDT
  case class TaxComponent(amount: Amount, taxable: Amount) extends BaseUDT
  case class ItemTax(total: Amount, components: List[TaxComponent]) extends BaseUDT
  case class Item(id: String, price: Option[Amount], quantity: Measure, pricing: Pricing, tax: String) extends BaseUDT
  case class Revision(id: String, Array: List[Item]) extends BaseUDT

  case class Period(timezone: String, starts: String, ends: String) extends BaseUDT
  case class Envelope(issued: String, country: String, region: String, party: String, period: Period) extends BaseUDT
  case class Invoice(_id: String, items: Array[Item], envelope: Envelope) extends BaseUDT

  case class Operand(`type`: String, value: String) extends BaseUDT
  case class RuleExpression(left: Operand, right: Operand, op: String) extends BaseUDT
  case class RuleExpressionWrapper(expr: RuleExpression) extends BaseUDT
  case class WhenRule(envelope: Option[Array[RuleExpressionWrapper]], item: Option[Array[RuleExpressionWrapper]]) extends BaseUDT
  case class RuleItem(whens: WhenRule) extends BaseUDT
  case class Rule(_id: String, rule_id: String, items: Array[RuleItem]) extends BaseUDT
}
