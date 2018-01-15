package org.xalgorithms.jobs

import scala.concurrent.duration.FiniteDuration
import org.xalgorithms.apps._

class EffectiveRulesJob(cfg: ApplicationConfig) extends KafkaStreamingApplication {
  override def config: Map[String, String] = cfg.spark
  override def batch_duration: FiniteDuration = cfg.batch_duration
  override def checkpoint_dir: String = cfg.checkpoint_dir

  def execute(): Unit = {
    with_context(cfg, { (ctx, sctx, input) =>
      input.map { s => s.reverse }
    })
  }
}

object EffectiveRulesJob {
  def main(args: Array[String]) : Unit = {
    val job = new EffectiveRulesJob(EffectiveRulesJobConfig())
    job.execute()
  }
}

object EffectiveRulesJobConfig {
  import com.typesafe.config.Config
  import com.typesafe.config.ConfigFactory
  import net.ceedubs.ficus.Ficus._

  def apply(): ApplicationConfig = apply(ConfigFactory.load)

  def apply(app_cfg: Config): ApplicationConfig = {
    val cfg = app_cfg.getConfig("EffectiveRules")
    new ApplicationConfig(
      cfg.as[String]("topics.input"),
      cfg.as[String]("topics.output"),
      cfg.as[Map[String, String]]("kafka.source"),
      cfg.as[Map[String, String]]("kafka.sink"),
      cfg.as[Map[String, String]]("spark"),
      cfg.as[FiniteDuration]("batch_duration"),
      cfg.as[String]("checkpoint_dir")
    )
  }
}
