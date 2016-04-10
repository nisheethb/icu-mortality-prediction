package edu.gatech.cse8803.features

/**
  * @author Nisheeth Bandaru
  * */

import edu.gatech.cse8803.model._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import java.util.Date

object FeatureConstruction {

  /**
    * ((subject-id, feature-name), feature-value)
    */
  type FeatureTuple = ((String, String), Double)

  def normalizeFeatures(icuEvents: RDD[PatientEvent]): RDD[NormalizedPatientEvent] = {

    val numericFeatures = icuEvents.map{
      f =>
        val subid = f.subject_id.toInt
        val sex: Int = {
          if (f.gender == "F")
            0
          else
            1
        }
        val hadmid: Int = {
          if (f.hadm_id.isEmpty)
            -1
          else
            f.hadm_id.toInt
        }
        val total_stay = f.icustay_total_num.toInt
        val stay_seqNum = f.subject_icustay_seq.toInt
        val sub_age: Double = {
          if (f.age.toDouble < 0 || f.age.toDouble > 120)
            -1.0
          else
            f.age.toDouble
        }
        val expiredInICU: Double = {
          if (f.icustay_expire_flg == "Y")
            1
          else
            0
        }

        val sapsi_f: Double = {
          if (f.sapsi_first.isEmpty)
            -1
          else
            f.sapsi_first.toDouble
        }

        NormalizedPatientEvent(subid, sex, hadmid, total_stay, stay_seqNum, sub_age, sapsi_f, expiredInICU)
    }

    // Filter bad/unnecessary data
    val filteredData = numericFeatures.filter(f => f.age >= 18).filter(f => f.sapsi_first >= 0)

    // Normalize everything to the same scale
    // Get avgs, min, max
    val minAge = filteredData.map(f => f.age).min()
    val maxAge = filteredData.map(f => f.age).max()
    val meanAge = filteredData.map(f => f.age).mean()

    val minSAPS = filteredData.map(f => f.sapsi_first).min().toDouble
    val maxSAPS = filteredData.map(f => f.sapsi_first).max().toDouble
    val meanSAPS = filteredData.map(f => f.sapsi_first).mean().toDouble

    val normalizedData = filteredData.map{
      f =>
        val normedAge = (f.age - meanAge)/(maxAge - minAge)
        val normedSAPS = (f.sapsi_first - meanSAPS)/(maxSAPS - minSAPS)

        NormalizedPatientEvent(f.subject_id, f.gender, f.hadm_id,
          f.icustay_total_num, f.icustay_seq_num, normedAge, normedSAPS, f.icustay_expire_flg)
    }

    normalizedData
  }

  def constructLPforStructured(normedPatientEvents:RDD[NormalizedPatientEvent]): RDD[LabeledPoint] = {
    val labeled = normedPatientEvents.map{
      f =>
        if (f.icustay_expire_flg == 0.0)
          LabeledPoint(0, Vectors.dense(f.age, f.gender.toDouble, f.sapsi_first))
        else
          LabeledPoint(1, Vectors.dense(f.age, f.gender.toDouble, f.sapsi_first))
    }

    labeled
  }

  def applytfidf(icuNotes:RDD[IcuEvent]): List[Int] = {

    val sc = icuNotes.sparkContext
    val notes = icuNotes.map( f => f.text)

    val corpus: RDD[String] = notes
    // Split each document into a sequence of terms (words)
    val tokenized: RDD[Seq[String]] =
      corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))

    // Choose the vocabulary.
    //   termCounts: Sorted list of (term, termCount) pairs
    val termCounts: Array[(String, Long)] =
      tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    //   vocabArray: Chosen vocab (removing common terms)
    val numStopwords = 20
    val vocabArray: Array[String] =
      termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
    //   vocab: Map term -> term index
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    val vocabsize = vocab.values.size
    println("vocab size ", vocabsize)

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
      tokenized.zipWithIndex.map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(vocab.size, counts.toSeq))
      }

    // Set LDA parameters
    val numTopics = 10
    val lda = new LDA().setK(numTopics).setMaxIterations(10)

    val ldaModel = lda.run(documents)
    val avgLogLikelihood = ldaModel.asInstanceOf[DistributedLDAModel].logLikelihood / documents.count()

    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    topicIndices.foreach { case (terms, termWeights) =>
      println("TOPIC:")
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${vocabArray(term.toInt)}\t$weight")
      }
      println()
    }

    List(1,2,3)
  }

}
