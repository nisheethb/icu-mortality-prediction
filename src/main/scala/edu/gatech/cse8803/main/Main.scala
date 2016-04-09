/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.jaccard.Jaccard
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.randomwalk.RandomWalk
import edu.gatech.cse8803.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import edu.gatech.cse8803.graphconstruct.GraphLoader


object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    val (patientDetails) = loadRddRawData(sqlContext)
    val avgAge:Double = patientDetails.map(l => l.age.toDouble).mean()
    println("avg age ", avgAge)


    sc.stop()
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[PatientEvent]) = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

    List("data/icustay_detail.csv", "data/noteevents.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val patientDetails = sqlContext.sql( // fix this
      """
        |SELECT subject_id, gender, hadm_id, icustay_total_num,
        |icustay_admit_age, icustay_expire_flg, sapsi_first, sofa_first
        |FROM icustay_detail
      """.stripMargin)
      .map(r => PatientEvent(r(0).toString, r(1).toString, r(2).toString, r(3).toString,
        r(4).toString, r(5).toString, r(6).toString, r(7).toString))


    (patientDetails)

  }


  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
