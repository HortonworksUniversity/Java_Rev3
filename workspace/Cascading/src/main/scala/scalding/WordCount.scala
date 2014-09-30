package scalding

import com.twitter.scalding._
import cascading.flow.tez.FlowProcessor

class WordCountJob(args: Args) extends Job(args) {
  TextLine(args("input"))
    .flatMap('line -> 'word) { line: String => line.split("""\s+""") }
    .groupBy('word) { _.size }
    .write(Tsv(args("output")))

  override def config: Map[AnyRef, AnyRef] = {
    val config = super.config
    if (args.boolean("tez")) {
      config ++ Map("cascading.flow.runtime.gather.partitions.num" -> "4",
        "tez.lib.uris" -> "hdfs:///apps/tez-0.5.0/tez-0.5.0.tar.gz",
        "cascading.app.appjar.class" -> this.getClass())
    } else {
      config ++ Map("cascading.app.appjar.class" -> this.getClass())
    }
  }

  override def run: Boolean = {
    val flow = buildFlow
    flow.complete

    if (!args.boolean("tez")) {
      val statsData = flow.getFlowStats
      handleStats(statsData)
      statsData.isSuccessful
    } else {
      true
    }
  }
}

