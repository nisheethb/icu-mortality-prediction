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
    val (patientDetails, comorbidityDetails, icuDetails) = loadRddRawData(sqlContext)
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

    // Use the structured features
    // Apply tf-idf
    val vectfidf = FeatureConstruction.vectorizeNotes(icuDetails)


    sc.stop()
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[PatientEvent], RDD[IcuEvent]) = {

    /** Load in the data */
    val dateFormat = new SimpleDateFormat("dd-MM-yyyy'  'hh:mm:ss a")

    List("data/icustay_detail.csv", "data/notesprocess.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val patientDetails = sqlContext.sql( // fix this
      """
        |SELECT subject_id, gender, hadm_id, icustay_total_num, subject_icustay_seq,
        |icustay_admit_age, icustay_expire_flg, sapsi_first, sapsi_min, sapsi_max
        |FROM icustay_detail
      """.stripMargin)
      .map(r => PatientEvent(r(0).toString, r(1).toString, r(2).toString, r(3).toString,
        r(4).toString, r(5).toString, r(6).toString, r(7).toString, r(8).toString, r(9).toString))

    val comorbidityDetails = sqlContext.sql(
      """
        |SELECT congestive_heart_failure, cardiac_arrhythmias, valvular_disease, pulmonary_circulation,
        |	peripheral_vascular, hypertension, paralysis, other_neurological, chronic_pulmonary,
        | diabetes_uncomplicated, diabetes_complicated,	hypothyroidism, renal_failure, liver_disease,	peptic_ulcer,
        | aids,	lymphoma,	metastatic_cancer, solid_tumor, rheumatoid_arthritis, coagulopathy, obesity, weight_loss,
        | fluid_electrolyte, blood_loss_anemia, deficiency_anemias, alcohol_abuse, drug_abuse, psychoses, depression
        |FROM comorbidity_scores
      """.stripMargin)
        .map(r => MorbidityEvent(r(0).toString, r(1).toString, r(2).toString, r(3).toString, r(4).toString,
          r(5).toString, r(6).toString, r(7).toString, r(8).toString, r(9).toString, r(10).toString,
          r(11).toString, r(12).toString, r(13).toString, r(14).toString, r(15).toString, r(16).toString,
          r(17).toString, r(18).toString, r(19).toString, r(20).toString, r(21).toString, r(22).toString,
          r(23).toString, r(24).toString, r(25).toString, r(26).toString, r(27).toString, r(28).toString,
          r(29).toString, r(30).toString))

    val icuDetails = sqlContext.sql(
      """
        |SELECT subject_id, text
        |FROM notesprocess
      """.stripMargin
    ).map(r => IcuEvent(r(0).toString, r(1).toString))

    (patientDetails, comorbidityDetails, icuDetails)

  }


  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Project App", "local")
}
