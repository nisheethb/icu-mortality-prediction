package edu.gatech.cse8803.features

/**
  * @author Nisheeth Bandaru
  * */

import edu.gatech.cse8803.model._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
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

  def applytfidf(icuNotes:RDD[IcuEvent]): RDD[Vector] = {

    val sc = icuNotes.sparkContext
    // Load documents (one per line).
    val documents: RDD[Seq[String]] = sc.textFile("data/sampledata.txt").map(_.split(" ").toSeq)

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    tfidf

  }

}
