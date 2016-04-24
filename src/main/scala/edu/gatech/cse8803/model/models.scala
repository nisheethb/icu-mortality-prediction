/**
 * @author Hang Su <hangsu@gatech.edu>.
 */
package edu.gatech.cse8803.model

import org.apache.spark.mllib.linalg.Vector

case class PatientEvent(subject_id:String, gender:String, hadm_id:String,
                        icustay_total_num:String, subject_icustay_seq:String, age:String,
                        icustay_expire_flg:String, sapsi_first:String, sapsi_min:String, sapsi_max:String)

case class IcuEvent(subject_id:String, text: String)

case class NoteEvent(subject_id:Long, document:Vector)

/**case class MorbidityEvent(subject_id:String, eh1:String, eh2:String, eh3:String, eh4:String, eh5:String,
                          eh6:String, eh7:String, eh8:String, eh9:String, eh10:String, eh11:String,
                          eh12:String, eh13:String, eh14:String, eh15:String, eh16:String, eh17:String,
                          eh18:String, eh19:String, eh20:String, eh21:String, eh22:String, eh23:String,
                          eh24:String, eh25:String, eh26:String, eh27:String, eh28:String, eh29:String,
                          eh30:String)
  */

case class NormalizedPatientEvent(subject_id: Int, gender: Int, hadm_id:Int, icustay_total_num:Int,
                                  icustay_seq_num: Int, age:Double, sapsi_first:Double, sapsi_min:Double,
                                  sapsi_max:Double, icustay_expire_flg:Double)




