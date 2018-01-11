package org.xalgorithms.discover_rules

import org.joda.time.DateTime

object udt {
  sealed trait BaseUDT

  case class Amount(value: Integer, currency_code: String) extends BaseUDT
  case class Measure(value: Integer, unit: String) extends BaseUDT
  case class Pricing(orderable_factor: Integer, price: Option[Amount], quantity: Measure) extends BaseUDT
  case class TaxComponent(amount: Amount, taxable: Amount) extends BaseUDT
  case class ItemTax(total: Amount, components: Array[TaxComponent]) extends BaseUDT
  case class Item(id: String, price: Option[Amount], quantity: Measure, pricing: Pricing, tax: String) extends BaseUDT

  case class Period(timezone: String, starts: String, ends: String) extends BaseUDT
  case class Envelope(issued: String, country: String, region: String, party: String, period: Period) extends BaseUDT
  case class Invoice(_id: String, items: Array[Item], envelope: Envelope) extends BaseUDT

  case class Effective(rule_id: String, country: String, region: String, party: String, timezone: String, starts: DateTime, ends: DateTime)
}
