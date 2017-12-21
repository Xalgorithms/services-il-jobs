package org.xalgorithms.discover_items

object udt {
  sealed trait BaseUDT

  case class Amount(value: Integer, currency_code: String) extends BaseUDT { def this() = this(value = null, currency_code = null) }
  case class Measure(value: Integer, unit: String) extends BaseUDT { def this() = this(value = null, unit = null) }
  case class Pricing(orderable_factor: Integer, price: Option[Amount], quantity: Measure) extends BaseUDT {
    def this() = this(orderable_factor = null, price = null, quantity = null)
  }
  case class TaxComponent(amount: Amount, taxable: Amount) extends BaseUDT { def this() = this(amount = null, taxable = null) }
  case class ItemTax(total: Amount, components: Array[TaxComponent]) extends BaseUDT { def this() = this(total = null, components = null) }
  case class Item(id: String, price: Option[Amount], quantity: Measure, pricing: Pricing, tax: String) extends BaseUDT {
    def this() = this(id = null, price = null, quantity = null, pricing = null, tax = null)
  }
  case class Invoice(_key: String, items: Array[Item]) extends BaseUDT { def this() = this(_key = null, items = null) }
}
