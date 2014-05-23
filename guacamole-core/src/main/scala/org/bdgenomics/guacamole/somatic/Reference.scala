package org.bdgenomics.guacamole.somatic

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.mapred.TextInputFormat
import scala.collection.mutable
import org.bdgenomics.guacamole.{ LociMapLongSingleContigSerializer, LociMap, Common }
import com.esotericsoftware.kryo.{ KryoSerializable, Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }

case class Reference(bases: RDD[(Reference.Locus, Byte)],
                     contigRanges: Map[String, (Long, Long)]) {}

object Reference {

  type Locus = (String, Long)

  case class LocusPartitioner(n: Int, contigRanges: mutable.Map[String, (Long, Long)]) extends Partitioner {

    // identity map on the end because serialization in Scala sucks
    val contigSizes = contigRanges.mapValues({ case (start, stop) => (stop - start) }).map(x => x)
    val totalNumLoci = contigSizes.values.reduce(_ + _)

    val (_, lociBeforeContig) = contigSizes.foldLeft((0L, Map[String, Long]())) {
      case ((total, map), (k, n)) =>
        val newTotal = total + n
        val newMap: Map[String, Long] = map + (k -> total)
        (newTotal, newMap)
    }

    def numPartitions =
      // in case n is too large
      Math.min(n, (totalNumLoci / 10000 + 1).toInt)

    val lociPerPartition: Long = totalNumLoci / numPartitions.toLong

    def getPartition(key: Any): Int = {
      assert(key.isInstanceOf[Locus], "Not a locus: %s".format(key))
      val (contig, offset): (String, Long) = key.asInstanceOf[Locus]
      val eltsBefore: Long = lociBeforeContig.getOrElse(contig, 0)
      val globalPos: Long = eltsBefore + offset
      return (globalPos / lociPerPartition).toInt
    }
  }

  class LocusPartitionerSerializer extends Serializer[LocusPartitioner] {

    def write(kryo: Kryo, output: Output, part: LocusPartitioner) {
      output.writeInt(part.n)
      output.writeInt(part.contigRanges.size)
      for ((k, (start, stop)) <- part.contigRanges) {
        output.writeString(k)
        output.writeLong(start)
        output.writeLong(stop)
      }
    }

    def read(kryo: Kryo, input: Input, t: Class[LocusPartitioner]): LocusPartitioner = {
      val numPartitions = input.readInt()
      val numContigs = input.readInt()
      val contigRanges = mutable.Map[String, (Long, Long)]()
      for (i <- 0 to numContigs) {
        val k = input.readString()
        val start = input.readLong()
        val stop = input.readLong()
        contigRanges(k) = (start, stop)
      }
      LocusPartitioner(numPartitions, contigRanges)
    }
  }
  /**
   *
   * Since formats/sources differ on whether to call a chromosome "chr1" vs. "1"
   * normalize them all the drop the 'chr' prefix (and to use "M" instead of "MT").
   *
   * @param contigName
   * @return
   */
  def normalizeContigName(contigName: String): String = {
    contigName.replace("chr", "").replace("MT", "M")
  }

  /**
   * Loads a FASTA file into an RDD[(K,V)] where
   * key K = (contig name : String, line in contig : Long)
   * value V = string  of nucleotides
   *
   * @param path
   * @param sc
   * @return
   */
  def loadReferenceLines(path: String, sc: SparkContext): (RDD[(Locus, Array[Byte])], Map[String, (Long, Long)]) = {

    // Hadoop loads a text file into an RDD of lines keyed by byte offsets
    val fastaByteOffsetsAndLines: RDD[(Long, Array[Byte])] =
      sc.hadoopFile[LongWritable, Text, TextInputFormat](path).map({
        case (x, y) => (x.get(), y.getBytes)
      })
    val sortedSequences: RDD[(Long, Array[Byte])] = fastaByteOffsetsAndLines.sortByKey(ascending = true)
    val numLines = sortedSequences.count
    val partitionSizes: Array[Long] = sortedSequences.mapPartitions({
      partition => Seq(partition.length.toLong).iterator
    }).collect()
    Common.progress("-- collected reference partition sizes")
    val partitionSizesBroadcast = sc.broadcast(partitionSizes)
    val numberedLines: RDD[(Long, Array[Byte])] = sortedSequences.mapPartitionsWithIndex {
      case (partitionIndex: Int, partition: Iterator[(Long, Array[Byte])]) =>
        val offset = if (partitionIndex > 0) partitionSizesBroadcast.value(partitionIndex - 1) else 0L
        partition.zipWithIndex.map({
          case ((_, bytes), i) => (i.toLong + offset, bytes)
        }).toIterator
    }

    //collect all the lines which start with '>'
    val referenceDescriptionLines: List[(Long, String)] =
      numberedLines.filter({
        case (lineNumber, bytes) =>
          bytes.length > 0 && bytes(0).toChar == '>'
      }).collect.map({
        case (lineNumber, bytes) =>
          (lineNumber, bytes.map(_.toChar).mkString)
      }).toList
    Common.progress("-- collected contig headers")
    //parse the contig description to just pull out the contig name
    val referenceContigNames: List[(Long, String)] =
      referenceDescriptionLines.map({
        case (lineNumber, description) =>
          val text = description.substring(1)
          val contigName = normalizeContigName(text.split(' ')(0))
          (lineNumber, contigName)
      })
    val referenceContigBytes = referenceContigNames.map(_._1)
    val referenceIndex = mutable.Map[String, (Long, Long)]()
    for ((start, contigName) <- referenceContigNames) {
      // stop of this contig is the start line of the next one
      val stopCandidates = referenceContigBytes.filter(_ > start)
      val stop = if (stopCandidates.length > 0) stopCandidates.min else numLines
      referenceIndex(contigName) = (start, stop)
    }
    // hand-waiving around performance of closure objects by broadcasting
    // every collection that should be shared by multiple workers
    val referenceIndexBroadcast = sc.broadcast(referenceIndex)
    Common.progress("-- broadcast reference index")

    // associate each line with whichever contig contains it
    val locusLines = numberedLines.flatMap({
      case (pos, seq) =>
        referenceIndexBroadcast.value.find({
          case (_, (start, stop)) =>
            (start < pos && stop > pos)
        }).map({
          case (contigName, (start, stop)) =>
            ((contigName, pos - start - 1), seq)
        })
    })
    val locusPartitioner: Partitioner = new LocusPartitioner(1000, referenceIndex)
    val repartitioned = locusLines.partitionBy(locusPartitioner)
    (repartitioned, referenceIndex.toMap)
  }

  def load(path: String, sc: SparkContext): Reference = {
    val (referenceLines, referenceIndex) = loadReferenceLines(path, sc)
    val bases = referenceLines.flatMap({ case (locus, bytes) => bytes.map((c: Byte) => (locus, c)) })
    Reference(bases, referenceIndex)
  }
}