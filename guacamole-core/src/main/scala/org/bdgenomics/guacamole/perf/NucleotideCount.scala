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
    @Option(name = "-count", usage = "Reduce RDD using countByKey instead of reduceByKey")
    var globalCount : Boolean = false

    @Option(name = "-sort", usage = "Sort before counting")
    var sort : Boolean = false
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc: SparkContext = Common.createSparkContext(args)
    Common.progress("Created Spark context")
    var reads: RDD[String] = sc.textFile(args.reads)
    Common.progress("Loaded text file")
    var sequences: RDD[String] = reads.filter(!_.startsWith("@")).map(_.split("\t")(9))
    Common.progress("Extracted read sequences")
    var localCounts: RDD[(Char, Long)] = sequences.flatMap(seq => seq.map(c => (c, 1L)))
    Common.progress("flatMap")
    if (args.sort) {
      localCounts = localCounts.sortByKey()
      Common.progress("sort")
    }
    val counts: Array[(Char, Long)] = if (args.globalCount) {
      println("-- countByKey")
      localCounts.countByKey().toArray
    } else {
      println("-- reduceByKey")
      localCounts.reduceByKey(_ + _).collect
    }
    Common.progress("global reduce")
    for ((c, i) <- counts) {
      println("%s : %d".format(c, i))
    }
  }
}
