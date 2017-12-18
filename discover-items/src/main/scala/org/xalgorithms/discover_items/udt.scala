package org.xalgorithms.discover_items

object udt {
  sealed trait BaseUDT

  case class Amount(value: BigDecimal, currency_code: String) extends BaseUDT
  case class Measure(value: BigDecimal, unit: String) extends BaseUDT
  case class Pricing(orderable_factor: BigDecimal, price: Option[Amount], quantity: Measure) extends BaseUDT
  case class TaxComponent(amount: Amount, taxable: Amount) extends BaseUDT
  case class ItemTax(total: Amount, components: List[TaxComponent]) extends BaseUDT
  case class Item(id: String, price: Option[Amount], quantity: Measure, pricing: Pricing, tax: String) extends BaseUDT
  case class Invoice(items: List[Item]) extends BaseUDT
}
