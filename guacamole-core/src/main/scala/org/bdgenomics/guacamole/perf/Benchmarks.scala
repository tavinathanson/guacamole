package org.bdgenomics.guacamole.perf

import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.bdgenomics.guacamole.Common
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.Command
import org.bdgenomics.guacamole.Common.Arguments.{ Reads, Base }
import org.kohsuke.args4j.Option
import net.sf.samtools.SAMRecord
import org.apache.hadoop.io.{ Text, LongWritable }
import fi.tkk.ics.hadoop.bam.{ AnySAMInputFormat, SAMRecordWritable }
import org.bdgenomics.adam.avro.ADAMRecord
import org.apache.hadoop.mapred.TextInputFormat

object Benchmarks extends Command {
  override val name = "perf"
  override val description = "Benchmark for counting nucleotides in a SAM file"

  private class Arguments extends Reads {

    @Option(name = "-sort-byte-offsets", usage = "Sort byte offsets of input records")
    var sortByteOffsets: Boolean = false

    @Option(name = "-sort-records", usage = "Sort SAM entries before extracting nucleotides")
    var sortRecords: Boolean = false

    @Option(name = "-sort-hash-codes", usage = "Sort SAM entries by hash codes")
    var sortHashCodes: Boolean = false

    @Option(name = "-sort-nucleotides", usage = "Sort nucleotides before counting")
    var sortChars: Boolean = false

    @Option(name = "-count", usage = "Use countByKey instead of sortByKey")
    var count: Boolean = false

    @Option(name = "-parallelism", usage = "Parallelism level to use instead of sc.defaultParallelism")
    var parallelism: Int = 0
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc: SparkContext = Common.createSparkContext(args)

    Common.progress("Created Spark context")
    println("Configuration: %s".format(sc.getConf.toDebugString))
    println("Default parallelism: %d".format(sc.defaultParallelism))
    println("Default min splits: %d".format(sc.defaultMinSplits))

    var sequences: RDD[String] = if (args.reads.endsWith("sam")) {
      // copied from textFile
      var byteOffsets: RDD[(Long, Text)] =
        sc.hadoopFile[LongWritable, Text, TextInputFormat](args.reads).map(x => (x._1.get, x._2))

      if (args.sortByteOffsets) {
        byteOffsets = byteOffsets.sortByKey()
        val firstVal = byteOffsets.first
        Common.progress("Sorted byte offsets (%d partitions, %s)".format(byteOffsets.partitions.length, byteOffsets.getClass))
      }
      var reads: RDD[String] = byteOffsets.map(_._2.toString)

      Common.progress(
        "Loaded text file (%d partitions, %s)".format(reads.partitions.length, reads.getClass))

      if (args.sortRecords) {
        val pairs: RDD[(String, Int)] = reads.map(s => (s, 0))
        reads = pairs.sortByKey().keys
        val firstVal = reads.first
        Common.progress(
          "Sorted strings (%d partitions, %s)".format(reads.partitions.length, reads.getClass))
      }

      if (args.sortHashCodes) {
        val hashCodes: RDD[(Int, String)] = reads.keyBy(_.hashCode)
        reads = hashCodes.sortByKey().values
        val firstVal = reads.first
        Common.progress(
          "sorted strings (%d partitions, %s)".format(reads.partitions, reads.getClass))
      }
      reads.filter(!_.startsWith("@")).map(_.split("\t")(9))
    } else if (args.reads.endsWith("bam")) {
      var byteOffsets: RDD[(LongWritable, SAMRecordWritable)] =
        sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](args.reads)
      if (args.sortByteOffsets) { byteOffsets = byteOffsets.sortByKey() }
      var records = byteOffsets.map(_._2.get)
      if (args.sortRecords) {
        records = records.keyBy(_.toString).sortByKey().values
      }
      if (args.sortHashCodes) {
        records = records.keyBy(_.hashCode()).sortByKey().values
      }
      byteOffsets.map({ case (k, v) => v.get.getReadString })
    } else {
      var adamRecords: RDD[ADAMRecord] = sc.adamLoad(args.reads)
      if (args.sortRecords) {
        adamRecords = adamRecords.keyBy(_.toString).sortByKey().values
      }
      if (args.sortHashCodes) {
        adamRecords = adamRecords.keyBy(_.hashCode()).sortByKey().values
      }
      adamRecords.map(_.getSequence.toString)
    }
    Common.progress(
      "Extracted nucleotide sequences (%d partitions, class %s)".format(
        sequences.partitions.length,
        sequences.getClass))

    var localCounts: RDD[(Char, Long)] = sequences.flatMap(seq => seq.map(c => (c, 1L)))

    Common.progress(
      "flatMap (%d partitions, class %s)".format(
        localCounts.partitions.length,
        localCounts.getClass))
    if (args.sortChars) {
      localCounts = localCounts.sortByKey()
      Common.progress("sort")
    }
    val counts: Array[(Char, Long)] = if (args.count) {
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
