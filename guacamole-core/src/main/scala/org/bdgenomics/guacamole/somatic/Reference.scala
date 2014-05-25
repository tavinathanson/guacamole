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

case class Reference(basesAtLoci: RDD[(Reference.Locus, Byte)],
                     index: Reference.Index) {
  val basesAtGlobalPositions: RDD[(Long, Byte)] =
    basesAtLoci.map({
      case (locus, nucleotide) =>
        val position: Long = index.locusToGlobalPosition(locus)
        (position, nucleotide)
    })
}

object Reference {

  type Locus = (String, Long)

  case class Index(contigSizes: Map[String, Long]) {
    val numLoci: Long = contigSizes.values.reduce(_ + _)
    val contigs = contigSizes.keys.toList
    val numContigs: Int = contigs.length
    val (_, contigStart) = contigs.foldLeft((0L, Map[String, Long]())) {
      case ((total, map), k) =>
        val n = contigSizes(k)
        val newTotal = total + n
        val newMap: Map[String, Long] = map + (k -> total)
        (newTotal, newMap)
    }

    val contigStartArray: Array[(Long, String)] = contigs.map {
      contig =>
        val start: Long = contigStart(contig)
        (start, contig)
    }.toArray

    def globalPositionToLocus(globalPosition: Long): Reference.Locus = {
      val numContigs = contigStartArray.length
      val lastContigIndex = numContigs - 1

      var lower = 0
      var upper = lastContigIndex

      // binary search to find which locus the position belongs to
      while (lower != upper) {
        // midpoint between i & j in indices
        val middle = (upper + lower) / 2
        val (currentPosition, currentContig) = contigStartArray(middle)

        if (currentPosition <= globalPosition) {
          // if the query position is between the current contig and the next one,
          // then just return the offset
          if ((middle < lastContigIndex) && (contigStartArray(middle + 1)._1 > globalPosition)) {
            return (currentContig, globalPosition - currentPosition)
          } else { lower = middle }
        } else { upper = middle }
      }
      assert(lower == upper)
      val (startPosition, contig) = contigStartArray(lower)
      (contig, globalPosition - startPosition)
    }

    def locusToGlobalPosition(locus: Reference.Locus): Long = {
      val (contig, offset) = locus
      val eltsBefore: Long = contigStart.getOrElse(contig, 0)
      eltsBefore + offset
    }

  }

  case class LocusPartitioner(n: Int, index: Index) extends Partitioner {

    def numPartitions = n

    val lociPerPartition: Long = index.numLoci / numPartitions.toLong

    def getPartition(key: Any): Int = {
      assert(key.isInstanceOf[Locus], "Not a locus: %s".format(key))
      val globalPos: Long = index.locusToGlobalPosition(key.asInstanceOf[Locus])
      (globalPos / lociPerPartition).toInt
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
  def loadReferenceLines(path: String, sc: SparkContext): (RDD[(Locus, Array[Byte])], Map[String, Long]) = {

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

    // start and stop bytes associated with each contig name
    val contigByteRanges = mutable.Map[String, (Long, Long)]()
    for ((start, contigName) <- referenceContigNames) {
      // stop of this contig is the start line of the next one
      val stopCandidates = referenceContigBytes.filter(_ > start)
      val stop = if (stopCandidates.length > 0) stopCandidates.min else numLines
      contigByteRanges(contigName) = (start, stop)
    }
    // hand-waiving around performance of closure objects by broadcasting
    // every collection that should be shared by multiple workers
    val byteRangesBroadcast = sc.broadcast(contigByteRanges)
    Common.progress("-- broadcast reference index")

    // associate each line with whichever contig contains it
    val locusLines = numberedLines.flatMap({
      case (pos, seq) =>
        byteRangesBroadcast.value.find({
          case (_, (start, stop)) =>
            (start < pos && stop > pos)
        }).map({
          case (contigName, (start, stop)) =>
            ((contigName, pos - start - 1), seq)
        })
    })
    val lineLengths: RDD[(String, Long)] = locusLines.map({ case ((contig, _), line) => (contig, line.length.toLong) })
    val contigSizes: Map[String, Long] = lineLengths.reduceByKey(_ + _).collectAsMap().toMap
    (locusLines, contigSizes)
  }

  /**
   * Load a FASTA reference file into a Reference object, which contans
   * an RDD of (Locus,Byte) for each nucleotide and a Map[Locus, (Long,Long)] of
   * start/stop
   * @param path
   * @param sc
   * @return
   */
  def load(path: String, sc: SparkContext): Reference = {
    val (referenceLines, contigSizes) = loadReferenceLines(path, sc)
    val index = Index(contigSizes)
    val bases = referenceLines.flatMap({ case (locus, bytes) => bytes.map((c: Byte) => (locus, c)) })
    Reference(bases, index)
  }
}