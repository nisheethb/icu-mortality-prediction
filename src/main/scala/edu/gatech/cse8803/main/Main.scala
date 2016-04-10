/**
 * @author Nisheeth Bandaru
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    val (patientDetails, icuDetails) = loadRddRawData(sqlContext)
    val normedFeatures = FeatureConstruction.normalizeFeatures(patientDetails)
    val filteredFeatures = normedFeatures.filter(f => f.icustay_seq_num == 1)

    val SDLabeledPoints = FeatureConstruction.constructLPforStructured(filteredFeatures)

    val splits = SDLabeledPoints.randomSplit(Array(0.7, 0.3), seed=8803L)
    val train = splits(0).cache()
    val test = splits(1).cache()

    val numIterations = 100
    val model = SVMWithSGD.train(train, numIterations)

    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)


    val icuCount = icuDetails.count()
    println("icu count", icuCount)
    sc.stop()
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[PatientEvent], RDD[IcuEvent]) = {

    val dateFormat = new SimpleDateFormat("dd-MM-yyyy'  'hh:mm:ss a")

    List("data/icustay_detail.csv", "data/notesprocess.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val patientDetails = sqlContext.sql( // fix this
      """
        |SELECT subject_id, gender, hadm_id, icustay_total_num, subject_icustay_seq,
        |icustay_admit_age, icustay_expire_flg, sapsi_first, sofa_first
        |FROM icustay_detail
      """.stripMargin)
      .map(r => PatientEvent(r(0).toString, r(1).toString, r(2).toString, r(3).toString,
        r(4).toString, r(5).toString, r(6).toString, r(7).toString, r(8).toString))

    val icuDetails = sqlContext.sql(
      """
        |SELECT subject_id, charttime, text
        |FROM notesprocess
      """.stripMargin
    ).map(r => IcuEvent(r(0).toString, r(1).toString, r(2).toString))

    (patientDetails, icuDetails)

  }


  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Project App", "local")
}
