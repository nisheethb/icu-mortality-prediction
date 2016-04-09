/**
  * @author Sungtae An <stan84@gatech.edu>.
  */

package edu.gatech.cse8803.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{PowerIterationClustering => PIC}

/**
  * Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by
  * [[http://www.icml2010.org/papers/387.pdf Lin and Cohen]]. From the abstract: PIC finds a very
  * low-dimensional embedding of a dataset using truncated power iteration on a normalized pair-wise
  * similarity matrix of the data.
  *
  * @see [[http://en.wikipedia.org/wiki/Spectral_clustering Spectral clustering (Wikipedia)]]
  */

object PowerIterationClustering {

  /** run PIC using Spark's PowerIterationClustering implementation
    *
    * @input: All pair similarities in the shape of RDD[(patientID1, patientID2, similarity)]
    * @return: Cluster assignment for each patient in the shape of RDD[(PatientID, Cluster)]
    *
    * */
  def runPIC(similarities: RDD[(Long, Long, Double)]): RDD[(Long, Int)] = {
    val sc = similarities.sparkContext

    val pic = new PIC()
      .setK(3)
      .setMaxIterations(100)

    val model = pic.run(similarities)

    //case class PatientCluster(patientID: Long, clusterLabel: Int)
    val clusterLabels = model.assignments.map{ f =>
     (f.id, f.cluster)
    }.collect.toList


    /** Remove placeholder code below and run Spark's PIC implementation */
    val clusteringResult = sc.parallelize(clusterLabels)

    /** Begin of sanity checks */
    val clusterCounts = clusteringResult.map(f => (f._2, 1.0)).reduceByKey(_ + _)
    clusterCounts.collect.foreach(println)
    //End of sanity checks  */

    clusteringResult
  }
}