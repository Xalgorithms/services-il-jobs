package org.xalgorithms.discover_rules

object udt {
  sealed trait BaseUDT

  case class Period(starts: String, ends: String) extends BaseUDT{ def this() = this(starts = null, ends = null) }
  case class Envelope(issued: String, period: Period, party: String, country: String, region: String) extends BaseUDT{
    def this() = this(issued = null, period = null, party = null, country = null, region = null)
  }
  case class Invoice(_key: String, envelope: Envelope) extends BaseUDT{ def this() = this(_key = null, envelope = null) }
}
