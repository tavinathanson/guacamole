package org.bdgenomics.guacamole.perf

import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.bdgenomics.guacamole.Common
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.Command
import org.bdgenomics.guacamole.Common.Arguments.{ Reads, Base }
import org.kohsuke.args4j.Option

object NucleotideCount extends Command {
  override val name = "perf-nucleotide-count"
  override val description = "Benchmark for counting nucleotides in a SAM file"

  private class Arguments extends Reads {
    @Option(name = "-globalCount", usage = "Reduce entire RDD")
    var globalCount : Boolean = true
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc: SparkContext = Common.createSparkContext(args)
    Common.progress("Created Spark context")
    var reads: RDD[String] = sc.textFile(args.reads)
    Common.progress("Loaded text file")
    val sequences: RDD[String] = reads.filter(!_.startsWith("@")).map(_.split("\t")(9))
    Common.progress("Extracted read sequences")
    val localCounts: RDD[(Char, Long)] = sequences.flatMap(seq => seq.map(c => (c, 1L)))
    Common.progress("flatMap")
    val counts: Array[(Char, Long)] = if (args.globalCount) {
      localCounts.reduceByKey(_ + _).collect
    } else {
      localCounts.countByKey().toArray
    }
    Common.progress("global reduce")
    for ((c, i) <- counts) {
      println("%s : %d".format(c, i))
    }
  }
}
