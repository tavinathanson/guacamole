package org.bdgenomics.guacamole.perf

import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.bdgenomics.guacamole.Common
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.Command
import org.bdgenomics.guacamole.Common.Arguments.{Reads, Base}


object NucleotideCount extends Command {
  override val name = "perf-nucleotide-count"
  override val description = "Benchmark for counting nucleotides in a SAM file"

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Reads](rawArgs)
    val sc : SparkContext = Common.createSparkContext(args)
    val reads : RDD[String] = sc.textFile(args.reads)
    val sequences : RDD[String] = reads.filter(!_.startsWith("@")).map(_.split("\t", 10)(9))
    val localCounts : RDD[(Char, Int)] = sequences.flatMap(seq => seq.map(c => (c, 1)))
    val counts : Array[(Char, Int)] = localCounts.reduceByKey(_ + _).collect
    for ((c,i) <- counts) {
      println("%s : %d".format(c,i))
    }
  }
}
