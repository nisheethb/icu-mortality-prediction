/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  /** Generate Bipartite Graph using RDDs
    *
    * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
    * @return: Constructed Graph
    *
    * */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    val sc = patients.sparkContext

    val labResultsLatest = labResults.map(l => ((l.patientID, l.labName), l))
      .reduceByKey((l1 , l2) => {if (l1.date > l2.date) l1 else l2})
      .map(l => LabResult(l._2.patientID, l._2.date, l._2.labName, l._2.value))

    val diaglatest = diagnostics.map(l => ((l.patientID, l.icd9code), l))
      .reduceByKey((l1 , l2) => {if (l1.date > l2.date) l1 else l2})
      .map(l => Diagnostic(l._2.patientID, l._2.date, l._2.icd9code, l._2.sequence))

    val medlatest = medications.map(l => ((l.patientID, l.medicine), l))
      .reduceByKey((l1 , l2) => {if (l1.date > l2.date) l1 else l2})
      .map(l => Medication(l._2.patientID, l._2.date, l._2.medicine))


    /** Check the counts of taking only the latest events
    println("patient count: ", patients.count())
    println("lab events count: ", labResults.count())
    println("diag events count: ", diagnostics.count())
    println("med events count: ", medications.count())
    println("lab results latest count: ", labResultsLatest.count())
    println("diag results latest count: ", diaglatest.count())
    println("med results latest count: ", medlatest.count())

      */

    /** HINT: See Example of Making Patient Vertices Below */
    val vertexPatient: RDD[(VertexId, VertexProperty)] = patients
      .map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))

    val startIndex_diag = patients.map(patient => patient.patientID.toLong).max() + 1
    println("max vertex patient: ", startIndex_diag)

    /** Make the vertices for diag */
    val diagVertexIdRDD = diagnostics
        .map(_.icd9code)
        .distinct
        .zipWithIndex
        .map{case (icd9code, zeroBasedIndex) =>
          (icd9code, zeroBasedIndex + startIndex_diag)}

    val startIndex_lab = diagVertexIdRDD.map(_._2.toLong).max() + 1

    val vertexDiag = diagVertexIdRDD
        .map{case(icd9code, index) => (index, DiagnosticProperty(icd9code))}
        .asInstanceOf[RDD[(VertexId, VertexProperty)]]


    /** Make the vertices for lab */
    val labVertexIdRDD = labResults
        .map(_.labName)
        .distinct
        .zipWithIndex
        .map{case (labName, zeroBasedIndex) =>
          (labName, zeroBasedIndex + startIndex_lab)}

    val startIndex_med = labVertexIdRDD.map(_._2.toLong).max() + 1

    val vertexLab = labVertexIdRDD
        .map{case(testName, index) => (index, LabResultProperty(testName))}
        .asInstanceOf[RDD[(VertexId, VertexProperty)]]


    /** Make the vertices for med */
    val medVertexRDD = medications
      .map(_.medicine)
      .distinct
      .zipWithIndex
      .map{case (medicine, zeroBasedIndex) =>
        (medicine, zeroBasedIndex + startIndex_med)}

    val endIndex_med = medVertexRDD.map(_._2.toLong).max()

    val vertexMed = medVertexRDD
      .map{case(medicine, index) => (index, MedicationProperty(medicine))}
      .asInstanceOf[RDD[(VertexId, VertexProperty)]]

    /** Some prints
    println("diag vertex count: ", vertexDiag.count())
    println("lab vertex count: ", vertexLab.count())
    println("med vertex count: ", vertexMed.count)
    println("diag vertex id start: ", startIndex_diag)
    println("lab vertex id start: ", startIndex_lab)
    println("med vertex id start: ", startIndex_med)
    println("med vertex id end: ", endIndex_med)
      */

    val diagnostic2VertexId = diagVertexIdRDD.collect.toMap
    val lab2VertexId = labVertexIdRDD.collect.toMap
    val med2VertexId = medVertexRDD.collect.toMap

    val bcDiagnostic2VertexId = sc.broadcast(diagnostic2VertexId)
    val bcMedication2VertexId = sc.broadcast(med2VertexId)
    val bcLab2VertexId = sc.broadcast(lab2VertexId)


    /** HINT: See Example of Making PatientPatient Edges Below
      *
      * This is just sample edges to give you an example.
      * You can remove this PatientPatient edges and make edges you really need
      * */
    case class PatientPatientEdgeProperty(someProperty: SampleEdgeProperty) extends EdgeProperty
    val edgePatientPatient: RDD[Edge[EdgeProperty]] = patients
      .map({p =>
        Edge(p.patientID.toLong, p.patientID.toLong, SampleEdgeProperty("sample").asInstanceOf[EdgeProperty])
      })

    val edgePatientDiagnostic: RDD[Edge[EdgeProperty]] = diaglatest
      .map{
        diag: Diagnostic => Edge(
          diag.patientID.toLong,
          bcDiagnostic2VertexId.value(diag.icd9code),
          PatientDiagnosticEdgeProperty(Diagnostic(diag.patientID, diag.date, diag.icd9code, diag.sequence)).asInstanceOf[EdgeProperty]
          )
      }
    val edgePatientDiagnostic2: RDD[Edge[EdgeProperty]] = diaglatest
      .map{
        diag: Diagnostic => Edge(
          bcDiagnostic2VertexId.value(diag.icd9code),
          diag.patientID.toLong,
          PatientDiagnosticEdgeProperty(Diagnostic(diag.patientID, diag.date, diag.icd9code, diag.sequence)).asInstanceOf[EdgeProperty]
        )
      }

    val edgePatientMedication: RDD[Edge[EdgeProperty]] = medlatest
      .map{
        med: Medication => Edge(
          med.patientID.toLong,
          bcMedication2VertexId.value(med.medicine),
          PatientMedicationEdgeProperty(Medication(med.patientID, med.date, med.medicine)).asInstanceOf[EdgeProperty]
        )
      }
    val edgePatientMedication2: RDD[Edge[EdgeProperty]] = medlatest
      .map{
        med: Medication => Edge(
          bcMedication2VertexId.value(med.medicine),
          med.patientID.toLong,
          PatientMedicationEdgeProperty(Medication(med.patientID, med.date, med.medicine)).asInstanceOf[EdgeProperty]
        )
      }

    val edgePatientLab: RDD[Edge[EdgeProperty]] = labResultsLatest
      .map{
        lab: LabResult => Edge(
          lab.patientID.toLong,
          bcLab2VertexId.value(lab.labName),
          PatientLabEdgeProperty(LabResult(lab.patientID, lab.date, lab.labName, lab.value)).asInstanceOf[EdgeProperty]
        )
      }
    val edgePatientLab2: RDD[Edge[EdgeProperty]] = labResultsLatest
      .map{
        lab: LabResult => Edge(
          bcLab2VertexId.value(lab.labName),
          lab.patientID.toLong,
          PatientLabEdgeProperty(LabResult(lab.patientID, lab.date, lab.labName, lab.value)).asInstanceOf[EdgeProperty]
        )
      }

    // union all the vertices
    val allVertices = sc.union(vertexPatient, vertexDiag, vertexMed, vertexLab)

    // union all the edges
    val allEdges = sc.union(edgePatientDiagnostic, edgePatientDiagnostic2, edgePatientLab, edgePatientLab2, edgePatientMedication, edgePatientMedication2)

    // Making Graph
    val graph: Graph[VertexProperty, EdgeProperty] = Graph(allVertices, allEdges)

    graph
  }
}
