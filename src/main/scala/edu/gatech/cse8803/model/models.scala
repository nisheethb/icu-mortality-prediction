/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

case class PatientEvent(subject_id:String, gender:String, hadm_id:String,
                        icustay_total_num:String, age:String, icustay_expire_flg:String,
                        sapsi_first:String, sofa_first:String)

