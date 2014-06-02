package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import org.apache.spark.Logging
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Output, Base }
import org.kohsuke.args4j.{ Option => Opt }
import scala.Option
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification
import org.bdgenomics.adam.avro.{ ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype }
import org.bdgenomics.guacamole.pileup.Pileup
import scala.collection.JavaConversions
import scala.Some
import org.bdgenomics.adam.avro.ADAMGenotypeAllele._
import scala.Some
import org.apache.spark
import org.apache.spark.mllib.regression.LabeledPoint

object SomaticCopycatVariantCaller extends Command with Serializable with Logging {
  override val name = "somatic-copycat"
  override val description = "approximate a arbitrary somatic variant caller by training on its calls"

  private class Arguments extends Base with Output with TumorNormalReads with DistributedUtil.Arguments {
    @Opt(name = "-model-output", metaVar = "X",
      usage = "")
    var modelOutput: String = ""

    @Opt(name = "-model-input", metaVar = "X",
      usage = "")
    var modelInput: String = ""

  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val (rawTumorReads, tumorDictionary, rawNormalReads, normalDictionary) =
      Common.loadTumorNormalReadsFromArguments(args, sc, mapped = true, nonDuplicate = true)

    assert(tumorDictionary == normalDictionary,
      "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
        .format(tumorDictionary, normalDictionary))

    val mappedTumorReads = rawTumorReads.map(_.getMappedRead).filter(_.mdTag.isDefined)
    val mappedNormalReads = rawNormalReads.map(_.getMappedRead).filter(_.mdTag.isDefined)

    mappedTumorReads.persist()
    mappedNormalReads.persist()

    Common.progress("Loaded %,d tumor mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      mappedTumorReads.count, mappedTumorReads.partitions.length))
    Common.progress("Loaded %,d normal mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      mappedNormalReads.count, mappedNormalReads.partitions.length))

    val loci = Common.loci(args, normalDictionary)

    /*
    if (args.modelInput.isEmpty) {
      // Training.
      val features = generateFeatures(mappedTumorReads, mappedNormalReads)
    } else {
      // Calling.
    }


    //val (thresholdNormal, thresholdTumor) = (args.thresholdNormal, args.thresholdTumor)
    val numGenotypes = sc.accumulator(0L)
    DelayedMessages.default.say { () => "Called %,d genotypes.".format(numGenotypes.value) }
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, mappedTumorReads, mappedNormalReads)


    val features: RDD[ADAMGenotype] = DistributedUtil.pileupFlatMapTwoRDDs[ADAMGenotype](
      mappedTumorReads,
      mappedNormalReads,
      lociPartitions,
      (pileupTumor, pileupNormal) => {

      })
    mappedTumorReads.unpersist()
    mappedNormalReads.unpersist()
    Common.writeVariantsFromArguments(args, genotypes)
    DelayedMessages.default.print()
    */
  }

  /*
  def generateFeatures(tumorReads: RDD[MappedRead], normalReads: RDD[MappedRead]): RDD[LabeledPoint] = {
    /*

     */

  }
  */
}
