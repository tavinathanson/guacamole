package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import org.apache.spark.{ SparkContext, Logging }
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Output, Base }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.rdd._
import org.bdgenomics.guacamole.pileup.Pileup
import scala.collection.{ mutable, JavaConversions }
import scala.Some
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.{ LogisticRegressionModel, LogisticRegressionWithSGD }
import org.apache.hadoop.fs.{ FileSystem, Path }
import java.io.{ InputStreamReader, BufferedReader, OutputStreamWriter, BufferedWriter }
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ArrayBuffer
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext
import scala.Some
import org.bdgenomics.guacamole.MappedRead
import org.apache.spark.mllib.regression.LabeledPoint

object SomaticTrainableVariantCaller extends Command with Serializable with Logging {
  override val name = "somatic-trainable"
  override val description = "approximate an arbitrary somatic variant caller by training on its calls"

  private class Arguments extends Base with Output with TumorNormalReads with DistributedUtil.Arguments {
    @Opt(name = "-train-model-output", metaVar = "X",
      usage = "")
    var trainModelOutput: String = ""

    @Opt(name = "-train-loci-called", metaVar = "X",
      usage = "")
    var trainLociCalled: String = ""

    @Opt(name = "-train-vcf", metaVar = "X",
      usage = "")
    var trainVCF: String = ""

    @Opt(name = "-train-num-iterations", metaVar = "X",
      usage = "")
    var trainNumIterations: Int = 100

    @Opt(name = "-predict-model-input", metaVar = "X",
      usage = "")
    var predictModelInput: String = ""

    @Opt(name = "-test-holdout-percent", metaVar = "X",
      usage = "Implies -test.")
    var testHoldOutPercent: Int = 0

    @Opt(name = "-test",
      usage = "")
    var test: Boolean = false

    @Opt(name = "-predict",
      usage = "")
    var predict: Boolean = false
  }
  type LocusLabel = (Long, Long)

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    if (args.testHoldOutPercent > 0) args.test = true

    val (rawTumorReads, tumorDictionary, rawNormalReads, normalDictionary) =
      Common.loadTumorNormalReadsFromArguments(args, sc, mapped = true, nonDuplicate = true)

    assert(tumorDictionary == normalDictionary,
      "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
        .format(tumorDictionary, normalDictionary))

    val reads = rawTumorReads.union(rawNormalReads).map(_.getMappedRead).filter(_.mdTag.isDefined)
    reads.persist()

    Common.progress("Loaded %,d tumor/normal mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      reads.count, reads.partitions.length))

    val loci = Common.loci(args, normalDictionary)
    val (trainingLoci, testLoci) = if (args.testHoldOutPercent > 0) {
      Common.progress("Splitting %,d loci into training and test sets.".format(loci.count))
      val trainingBuilder = LociSet.newBuilder
      val testBuilder = LociSet.newBuilder
      loci.contigs.foreach(contig => {
        loci.onContig(contig).individually.foreach(locus => {
          val builder = if (math.random * 100 > args.testHoldOutPercent) trainingBuilder else testBuilder
          builder.put(contig, locus, locus + 1)
        })
      })
      (trainingBuilder.result, testBuilder.result)
    } else {
      (loci, loci)
    }
    Common.progress("Using %,d loci for training and %,d loci for testing.".format(
      trainingLoci.count, testLoci.count))

    val contigToNum = sc.broadcast(loci.contigs.sorted.zipWithIndex.toMap)
    val numToContig = sc.broadcast(contigToNum.value.map(pair => (pair._2, pair._1)))
    def labelLocus(contig: String, locus: Long): LocusLabel = {
      (contigToNum.value(contig), locus)
    }
    val labels = getLabelsFromArgs(args, sc, loci)
    val broadcastLabels = sc.broadcast(labels)

    def labelFeatures(features: RDD[(LocusLabel, Array[Double])]): RDD[LabeledPoint] = {
      features.map({
        case ((contigNum, locus), features) => {
          val contig = numToContig.value(contigNum.toInt)
          val label = broadcastLabels.value.onContig(contig).get(locus).get.toDouble
          LabeledPoint(label, features)
        }
      })
    }

    val model = if (args.predictModelInput.nonEmpty) {
      readModel(args)
    } else {
      // Training.
      val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, testLoci, reads)
      val features = getFeatures(args, lociPartitions, reads, labelLocus)
      val labeledFeatures = labelFeatures(features)
      val model = LogisticRegressionWithSGD.train(labeledFeatures, args.trainNumIterations)
      Common.progress("Done training model.")
      Common.progress("Model: %s".format(model.toString))
      model
    }
    // Write model if the user requested it.
    maybeWriteModel(args, model)

    if (args.test) {
      Common.progress("Testing.")
      val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, trainingLoci, reads)
      val features = getFeatures(args, lociPartitions, reads, labelLocus).sortByKey()
      val labeledFeatures = labelFeatures(features)
      val truthPredictionPairs = labeledFeatures.map(point => {
        (point.label > .5, model.predict(point.features) > .5)
      })
      val counts = truthPredictionPairs.countByValue.toMap.withDefaultValue(0L)
      println("Counts: %s".format(counts.toString))
      def printCount(label: String, numerator: Long, denominator: Long) = {
        println("%s: %,d / %,d = %,2f%%".format(label, numerator, denominator, numerator * 100.0 / denominator))
      }
      println("**************************** TEST RESULTS *****************************")
      printCount("Called positive / true positives", counts((true, true)), counts((true, true)) + counts((true, false)))
      printCount("Called negative / true negatives", counts((false, false)), counts((false, true)) + counts((false, false)))
      println("***********************************************************************")
    }

    if (args.predict) {
      Common.progress("Predicting.")
      val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, reads)
      val features = getFeatures(args, lociPartitions, reads, labelLocus).sortByKey()
      val locusAndPredictions = features.map(pair => (pair._1, model.predict(pair._2)))
      val called = locusAndPredictions.filter(pair => pair._2 > .5).map(_._1).collect

      val calledBuilder = LociSet.newBuilder
      called.foreach({
        case (contigNum, locus) => {
          calledBuilder.put(numToContig.value(contigNum.toInt), locus, locus + 1)
        }
      })
      val calledLoci = calledBuilder.result

      Common.progress("Called %,d loci.".format(calledLoci.count))
      println(calledLoci.truncatedString(1000))
    }
    DelayedMessages.default.print()
  }

  def maybeWriteModel(args: Arguments, model: LogisticRegressionModel) = {
    if (args.trainModelOutput.isEmpty) {
      Common.progress("Not writing model: no output file specified.")
    } else {
      val filesystem = FileSystem.get(new Configuration())
      val path = new Path(args.trainModelOutput)
      val writer = new BufferedWriter(new OutputStreamWriter(filesystem.create(path, true)))
      writer.write("%f\n".format(model.intercept))
      model.weights.foreach(weight => writer.write("%f\n".format(weight)))
      writer.close()
      Common.progress("Wrote: %s".format(args.trainModelOutput))
    }
  }

  def readModel(args: Arguments): LogisticRegressionModel = {
    Common.progress("Loading model.")
    val filesystem = FileSystem.get(new Configuration())
    val path = new Path(args.predictModelInput)
    val reader = new BufferedReader((new InputStreamReader(filesystem.open(path))))
    val intercept = reader.readLine().toDouble
    var line = reader.readLine()
    val weights = new ArrayBuffer[Double]
    while (line != null) {
      val weight = line.toDouble
      weights += weight
    }
    Common.progress("Loaded model with %,d weights.".format(weights.length))
    new LogisticRegressionModel(weights.toArray, intercept)
  }

  def getLabelsFromArgs(args: Arguments, sc: SparkContext, allLoci: LociSet): LociMap[Long] = {
    val calledLoci: LociSet = if (args.trainVCF.nonEmpty) {
      Common.progress("Loading training labels from VCF")
      val variants = ADAMVariationContext.sparkContextToADAMVariationContext(sc).adamVCFLoad(args.trainVCF)
      variants.mapPartitions(iterator => {
        val builder = LociSet.newBuilder
        iterator.foreach(context => {
          val locus = context.position.pos
          builder.put(context.position.referenceName, locus, locus + 1)
        })
        Iterator(builder.result)
      }).reduce(_.union(_))
    } else {
      Common.progress("Parsing training labels.")
      LociSet.parse(args.trainLociCalled)
    }
    val builder = LociMap.newBuilder[Long]
    builder.put(allLoci, 0)
    builder.put(calledLoci, 1)
    val result = builder.result
    Common.progress("Loaded training labels: %,d calls of %,d total loci".format(calledLoci.count, allLoci.count))
    Common.progress("Training calls: %s".format(result.truncatedString(500)))
    result
  }

  def getFeatures(
    args: Arguments,
    lociPartitions: LociMap[Long],
    reads: RDD[MappedRead],
    labeler: (String, Long) => LocusLabel): RDD[(LocusLabel, Array[Double])] = {

    val pileupPoints: RDD[(LocusLabel, Array[Double])] = DistributedUtil.pileupFlatMap[(LocusLabel, Array[Double])](
      reads,
      lociPartitions,
      (contig, locus, pileup) => {
        val pileupTumor = pileup.byToken(1)
        val pileupNormal = pileup.byToken(2)
        val locusLabel = labeler(contig, locus)
        val features = PileupBasedFeatures.getAll(pileupTumor, pileupNormal)
        Iterator((locusLabel, features))
      })
    pileupPoints
  }



  object FeatureGroups {
    trait FeatureGroup {
      val names: Seq[String]
      val pileupBased: Boolean
    }
    trait PileupBasedFeatureGroup extends FeatureGroup {
      val pileupBased = true
      def apply(tumor: Pileup, normal: Pileup): Iterable[Double]
      def compute(tumor: Pileup, normal: Pileup): Iterable[Double]
    }


    val groups = Seq(
      PercentDifferences)

    def getAll(pileupTumor: Pileup, pileupNormal: Pileup): Array[Double] = {
      val result = new ArrayBuffer[Double]
      PileupBasedFeatures.features.foreach(feature => {
        result ++= feature(pileupTumor, pileupNormal)
      })
      result.result.toArray
    }

    object PercentDifferences extends PileupBasedFeatureGroup {
      val names = Bases.standardBases.map(base => "Base: %s".format(base.toString))

      // Percent differences in evidence for each base
      override def apply(tumor: Pileup, normal: Pileup): Iterable[Double] = {
        val possibleAllelesTumor = SomaticThresholdVariantCaller.possibleSNVAllelePercents(tumor)
        val possibleAllelesNormal = SomaticThresholdVariantCaller.possibleSNVAllelePercents(normal)
        Bases.standardBases.map(base => possibleAllelesTumor(base) - possibleAllelesNormal(base)).sorted
      }
    }


  }

}
