/**

  * students: please put your implementation in this file!
  **/
package edu.gatech.cse8803.jaccard

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /**
      * Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients.
      * Return a List of patient IDs ordered by the highest to the lowest similarity.
      * For ties, random order is okay
    */

    val otherPatients = graph.vertices
      .filter(_._2.isInstanceOf[PatientProperty])
      .map(_._2.asInstanceOf[PatientProperty])
      .map(_.patientID.toLong)
      .filter(p => p != patientID)

    val maxPatientID = graph.vertices
      .filter(_._2.isInstanceOf[PatientProperty])
      .map(_._2.asInstanceOf[PatientProperty])
      .map(_.patientID.toLong)
      .max()

    val patientVertices = graph.vertices.filter(_._2.isInstanceOf[PatientProperty])

    val patientneigborids = graph.collectNeighborIds(EdgeDirection.Out).filter(p => p._1 <= maxPatientID)
    val patientAneighbors = patientneigborids.filter(p => p._1 == patientID).map(p => p._2)


    /** Remove this placeholder and implement your code */
    List(1,2,3,4,5)
  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
      * Given a patient, med, diag, lab graph, calculate pairwise similarity between all
      * patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where
      * patient-1-id < patient-2-id to avoid duplications
    */

    val sc = graph.edges.sparkContext
    sc.parallelize(Seq((1L, 2L, 0.5d), (1L, 3L, 0.4d)))
  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /**
      * Helper function

      * Given two sets, compute its Jaccard similarity and return its result.
      * If the union part is zero, then return 0.
    */
    val aub = (a union b).size
    val aib = (a intersect b).size
    var sij:Double = 0.0
      if (aub > 0)
        {
          sij = aib.toDouble / aub.toDouble
    }
    /** Remove this placeholder and implement your code */
    sij
  }
}
