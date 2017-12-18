package org.xalgorithms.apply_rules

import spray.json.DefaultJsonProtocol

object udt {
  case class Amount(value: BigDecimal, currency_code: String)
  case class Measure(value: BigDecimal, unit: String)
  case class Pricing(orderable_factor: BigDecimal, price: Option[Amount], quantity: Measure)
  case class TaxComponent(amount: Amount, taxable: Amount)
  case class ItemTax(total: Amount, components: List[TaxComponent])
  case class Item(id: String, price: Option[Amount], quantity: Measure, pricing: Pricing, tax: String)
  case class Revision(id: String, items: List[Item])

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val amountFormat = jsonFormat2(Amount)
    implicit val measureFormat = jsonFormat2(Measure)
    implicit val pricingFormat = jsonFormat3(Pricing)
    implicit val itemFormat = jsonFormat5(Item)
    implicit val objFormat = jsonFormat2(Revision)
  }
}
