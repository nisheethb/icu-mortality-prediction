/**
 * @author Nisheeth Bandaru
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat
import java.util.Calendar

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.model._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
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

    import sqlContext.implicits._

    println(Calendar.getInstance().getTime.toString)
    /** initialize loading of data */
    val (patientDetails, icuDetails) = loadRddRawData(sqlContext)
    val normedFeatures = FeatureConstruction.normalizeFeatures(patientDetails).filter(f => f.icustay_seq_num == 1)

    println(Calendar.getInstance().getTime.toString,  "Loaded and processed data")
    // Admission Baseline Model - 3 features -------------------------------------------------

    val AdmBase_LP = FeatureConstruction.construct_LP_for_AdmBaseline(normedFeatures)

    val splits_ABL = AdmBase_LP.randomSplit(Array(0.7, 0.3), seed=8803L)
    val train_ABL = splits_ABL(0).cache()
    val test_ABL = splits_ABL(1).cache()

    val numIterations = 100
    val model_ABL = SVMWithSGD.train(train_ABL, numIterations)
    model_ABL.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels_ABL = test_ABL.map { point =>
      val score = model_ABL.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics_ABL = new BinaryClassificationMetrics(scoreAndLabels_ABL)
    val auROC_ABL = metrics_ABL.areaUnderROC()

    println(Calendar.getInstance().getTime.toString)
    println("Area under ROC for Admission Baseline = " + auROC_ABL)

    val PatientNoteTopics = FeatureConstruction.vectorizeNotes(icuDetails).toDF()
    println(Calendar.getInstance().getTime.toString)
    println("LDA done")

    PatientNoteTopics.registerTempTable("patientNotes")

    val patientDs = normedFeatures.toDF()
    patientDs.registerTempTable("patient_d")

    val dpatientTopics = sqlContext.sql(
      """
        |SELECT d.subject_id, d.icustay_expire_flg, d.gender, d.age, d.sapsi_first,
        | d.sapsi_min, d.sapsi_max, p.document
        |FROM patientNotes p
        |JOIN patient_d d
        |ON p.subject_id = d.subject_id
      """.stripMargin)

    println(Calendar.getInstance().getTime.toString)
    println("Joined the data sources")

    // Retrospective Topic Model - 50 features -------------------------------------
    val RTopics_LP = dpatientTopics.rdd.map(row => LabeledPoint(
      row.getAs[Double](1),
      row.getAs[Vector]{7}
    ))

    val splits_RT = RTopics_LP.randomSplit(Array(0.7, 0.3), seed=8803L)
    val train_RT = splits_RT(0).cache()
    val test_RT = splits_RT(1).cache()

    val model_RT = SVMWithSGD.train(train_RT, numIterations)

    model_RT.clearThreshold()


    // Compute raw scores on the test set.
    val scoreAndLabels_RT = test_RT.map { point =>
      val score = model_RT.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics_RT = new BinaryClassificationMetrics(scoreAndLabels_RT)
    val auROC_RT = metrics_RT.areaUnderROC()

    println(Calendar.getInstance().getTime.toString)
    println("Area under ROC for Retrospective Topics = " + auROC_RT)

    // Retrospective Topic + Admission Model - 53 features ---------------------------
    // first combine the two vectors
    val topicadm_features = dpatientTopics.rdd.map(row =>
      (row.getAs[Double](1),
        row.getAs[Double](2),
        row.getAs[Double](3),
        row.getAs[Double](4),
      row.getAs[Vector](7))
    )

    val topicADM_LP = FeatureConstruction.construct_LP_for_TopicADM(topicadm_features)
    val splits_tADM = topicADM_LP.randomSplit(Array(0.7, 0.3), seed=8803L)
    val train_tADM = splits_tADM(0).cache()
    val test_tADM = splits_tADM(1).cache()

    val model_tADM = SVMWithSGD.train(train_tADM, numIterations)

    model_tADM.clearThreshold()


    // Compute raw scores on the test set.
    val scoreAndLabels_tADM = test_tADM.map { point =>
      val score = model_tADM.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics_tADM = new BinaryClassificationMetrics(scoreAndLabels_tADM)
    val auROC_tADM = metrics_tADM.areaUnderROC()

    println(Calendar.getInstance().getTime.toString)
    println("Area under ROC for Retrospective Topics and Admission Model = " + auROC_tADM)

    sc.stop()
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[PatientEvent], RDD[IcuEvent]) = {

    /** Load in the data */
    val dateFormat = new SimpleDateFormat("dd-MM-yyyy'  'hh:mm:ss a")

    List("data/icustay_detail.csv", "data/comorbidity_scores.csv", "data/notesprocess.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val patientDetails = sqlContext.sql( // fix this
      """
        |SELECT subject_id, gender, hadm_id, icustay_total_num, subject_icustay_seq,
        |icustay_admit_age, icustay_expire_flg, sapsi_first, sapsi_min, sapsi_max
        |FROM icustay_detail
      """.stripMargin)
      .map(r => PatientEvent(r(0).toString, r(1).toString, r(2).toString, r(3).toString,
        r(4).toString, r(5).toString, r(6).toString, r(7).toString, r(8).toString, r(9).toString))

    /**
    *val comorbidityDetails = sqlContext.sql(
      *"""
        *|SELECT congestive_heart_failure, cardiac_arrhythmias, valvular_disease, pulmonary_circulation,
        *|	peripheral_vascular, hypertension, paralysis, other_neurological, chronic_pulmonary,
        *| diabetes_uncomplicated, diabetes_complicated,	hypothyroidism, renal_failure, liver_disease,	peptic_ulcer,
        *| aids,	lymphoma,	metastatic_cancer, solid_tumor, rheumatoid_arthritis, coagulopathy, obesity, weight_loss,
        *| fluid_electrolyte, blood_loss_anemia, deficiency_anemias, alcohol_abuse, drug_abuse, psychoses, depression
      *|FROM comorbidity_scores
      *""".stripMargin)
        *.map(r => MorbidityEvent(r(0).toString, r(1).toString, r(2).toString, r(3).toString, r(4).toString,
          *r(5).toString, r(6).toString, r(7).toString, r(8).toString, r(9).toString, r(10).toString,
          *r(11).toString, r(12).toString, r(13).toString, r(14).toString, r(15).toString, r(16).toString,
          *r(17).toString, r(18).toString, r(19).toString, r(20).toString, r(21).toString, r(22).toString,
          *r(23).toString, r(24).toString, r(25).toString, r(26).toString, r(27).toString, r(28).toString,
          *r(29).toString, r(30).toString))
      */

    val icuDetails = sqlContext.sql(
      """
        |SELECT subject_id, text
        |FROM notesprocess
      """.stripMargin
    ).map(r => IcuEvent(r(0).toString, r(1).toString))

    (patientDetails,  icuDetails)

  }


  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Project App", "local")
}
