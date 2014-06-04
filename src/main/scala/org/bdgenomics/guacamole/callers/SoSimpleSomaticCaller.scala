package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import org.apache.spark.Logging
import org.bdgenomics.guacamole.Common.Arguments.{ Output, TumorNormalReads }
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.avro.{ ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype }
import scala.Some
import org.bdgenomics.guacamole.pileup.Pileup
import scala.collection.JavaConversions

/**
 * Simple subtraction based somatic variant caller
 *
 * This takes two variant callers, calls variants on tumor and normal independently
 * and outputs the variants in the tumor sample BUT NOT the normal sample
 *
 * This assumes that both read sets only contain a single sample, otherwise we should compare
 * on a sample identifier when joining the genotypes
 *
 */
object SoSimpleSomaticCaller extends Command with Serializable with Logging {
  override val name = "debug-somatic"
  override val description = "call somatic variants using RDDs"

  private class Arguments extends DistributedUtil.Arguments with Output with TumorNormalReads {

  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val (normalReads, normalSequenceDictionary) = Read.loadReadRDDAndSequenceDictionaryFromBAM(args.normalReads, sc, mapped = true, nonDuplicate = true)
    val (tumorReads, tumorSequenceDictionary) = Read.loadReadRDDAndSequenceDictionaryFromBAM(args.tumorReads, sc, mapped = true, nonDuplicate = true)

    val genotypes: RDD[ADAMGenotype] = callSomaticVariants(normalReads, tumorReads, args, normalSequenceDictionary)
    Common.progress("Computed %,d genotypes".format(genotypes.count))

    Common.writeVariantsFromArguments(args, genotypes)
    DelayedMessages.default.print()
  }

  def callSomaticVariants(normalReads: RDD[Read], tumorReads: RDD[Read],
                          args: SoSimpleSomaticCaller.Arguments,
                          normalSequenceDictionary: SequenceDictionary): RDD[ADAMGenotype] = {

    val allReads = normalReads.map(_.getMappedRead()).union(tumorReads.map(_.getMappedRead()))
    val loci = Common.loci(args, normalSequenceDictionary)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, allReads)

    val genotypes: RDD[ADAMGenotype] = DistributedUtil.pileupFlatMap[ADAMGenotype](
      allReads,
      lociPartitions,
      pileup => callVariantsAtLocus(pileup).iterator)
    genotypes
  }

  /**
   * Computes the genotype and probability at a given locus
   *
   * @param pileup Collection of pileup elements at align to the locus
   * @return Sequence of possible called genotypes for all samples
   */
  def callVariantsAtLocus(pileup: Pileup): Seq[ADAMGenotype] = {
    val totalReads = pileup.elements.length
    val matchesOrMismatches = pileup.elements.filter(e => e.isMatch || e.isMismatch)
    val counts = matchesOrMismatches.map(_.sequencedSingleBase).groupBy(char => char).mapValues(_.length)
    val maxAllele = if (counts.size > 0) Some(counts.maxBy(_._2)) else None

    def variant(alternateBase: Byte): ADAMGenotype = {
      ADAMGenotype.newBuilder
        .setSampleId("default")
        .setVariant(ADAMVariant.newBuilder
          .setPosition(pileup.locus)
          .setReferenceAllele(Bases.baseToString(pileup.referenceBase))
          .setVariantAllele(Bases.baseToString(alternateBase))
          .setContig(ADAMContig.newBuilder.setContigName(pileup.referenceName).build)
          .build)
        .build
    }
    if (maxAllele.isDefined && maxAllele.get._1 != pileup.referenceBase && (maxAllele.get._2.toFloat / totalReads) > 0.4) {
      return Seq(variant(maxAllele.get._1))
    } else {
      Seq.empty
    }
  }

}

