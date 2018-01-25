package org.xalgorithms.jobs

import org.xalgorithms.apps._

class EffectiveRules(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg) {
  def execute(): Unit = {
    with_context(cfg, { (ctx, sctx, input) =>
      input.map { s => s.reverse }
    })
  }
}

object EffectiveRules {
  def main(args: Array[String]) : Unit = {
    val job = new EffectiveRules(ApplicationConfig("EffectiveRules"))
    job.execute()
  }
}
