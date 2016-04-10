/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

case class PatientEvent(subject_id:String, gender:String, hadm_id:String,
                        icustay_total_num:String, subject_icustay_seq:String, age:String,
                        icustay_expire_flg:String, sapsi_first:String, sofa_first:String)

case class NormalizedPatientEvent(subject_id: Int, gender: Int, hadm_id:Int,
                                  icustay_total_num:Int, icustay_seq_num: Int, age:Double,
                                  sapsi_first:Double, icustay_expire_flg:Double)

case class IcuEvent(subject_id:String, charttime:String, text: String)

