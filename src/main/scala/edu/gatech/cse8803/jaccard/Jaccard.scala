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

    val maxPatientID = graph.vertices
      .filter(_._2.isInstanceOf[PatientProperty])
      .map(_._2.asInstanceOf[PatientProperty])
      .map(_.patientID.toLong)
      .max()

    val patientAndNeighbors = graph.collectNeighborIds(EdgeDirection.Out).filter(p => p._1 <= maxPatientID)
    val patientAndNeighbors2 = graph.collectNeighborIds(EdgeDirection.Out)
      .filter(p => p._1 <= maxPatientID)
      .filter(p => p._1 != patientID)

    // for patientID, get their neighbors:
    val patientA_Attrs = patientAndNeighbors.filter(p => p._1 == patientID).map(_._2)
    val a_attrs = patientA_Attrs.collect.flatten.toSet

    val pands = patientAndNeighbors2.map{
      f =>
        val bid = f._1
        val b_attrs = f._2.toSet
        val jacSim = jaccard(a_attrs, b_attrs)

        (bid, jacSim)
    }

    /** Begin Sanity checks
    val onevallcount = pands.count()
    println("one v all count and values below: ", onevallcount)
    val top10printing = pands.top(15){
      Ordering.by(_._2)
    }.toList
    top10printing.foreach(println)

    End sanity checks */

    val top10p = pands.top(10){
      Ordering.by(_._2)
    }.map(x => x._1).toList

    top10p
  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
      * Given a patient, med, diag, lab graph, calculate pairwise similarity between all
      * patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where
      * patient-1-id < patient-2-id to avoid duplications
    */

    val sc = graph.edges.sparkContext

    val maxPatientID = graph.vertices
      .filter(_._2.isInstanceOf[PatientProperty])
      .map(_._2.asInstanceOf[PatientProperty])
      .map(_.patientID.toLong)
      .max()

    val patientAndNeighbors = graph.collectNeighborIds(EdgeDirection.Out).filter(p => p._1 <= maxPatientID)

    val patientPairs = patientAndNeighbors.cartesian(patientAndNeighbors)
      .filter{case (a, b) => a._1 != b._1}
      .filter{case (a, b) => a._1 < b._1}


    val sims = patientPairs.map{
      f =>
        val srcId = f._1._1
        val dstId = f._2._1

        val srcAttrs = f._1._2.toSet
        val dstAttrs = f._2._2.toSet
        val jSim = jaccard(srcAttrs, dstAttrs)

        (srcId, dstId, jSim)
    }

    /** Begin of sanity checks

    val top10all = sims.top(10){
      Ordering.by(_._3)
    }.toList
    val allvallcount = sims.count()
    println("allvall ccount and values below: ", allvallcount)
    top10all.foreach(println)
    end of sanity checks */

    val allSims = sims.collect.toList

    sc.parallelize(allSims)

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

    sij
  }
}
