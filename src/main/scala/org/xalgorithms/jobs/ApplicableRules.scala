package org.xalgorithms.jobs

import org.xalgorithms.apps._

class ApplicableRules(cfg: ApplicationConfig) extends KafkaStreamingApplication(cfg) {
  def execute(): Unit = {
    with_context(cfg, { (ctx, sctx, input) =>
      input.map { s => s.reverse }
    })
  }
}

object ApplicableRules {
  def main(args: Array[String]) : Unit = {
    val job = new ApplicableRules(ApplicationConfig("ApplicableRules"))
    job.execute()
  }
}
