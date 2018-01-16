package org.xalgorithms.jobs

import org.xalgorithms.apps._

class EffectiveRulesJob(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg) {
  def execute(): Unit = {
    with_context(cfg, { (ctx, sctx, input) =>
      input.map { s => s.reverse }
    })
  }
}

object EffectiveRulesJob {
  def main(args: Array[String]) : Unit = {
    val job = new EffectiveRulesJob(ApplicationConfig("EffectiveRules"))
    job.execute()
  }
}
