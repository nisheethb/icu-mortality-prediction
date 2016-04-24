package edu.gatech.cse8803.features

/**
  * @author Nisheeth Bandaru
  * */

import breeze.linalg.{DenseVector, SparseVector}
import edu.gatech.cse8803.model._
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import java.util.Date

object FeatureConstruction {

  /**
    * ((subject-id, feature-name), feature-value)
    */
  type FeatureTuple = ((String, String), Double)

  def normalizeFeatures(icuEvents: RDD[PatientEvent]): RDD[NormalizedPatientEvent] = {
    /** Clean data and normalize all the features to be in the same range
      * Parse numeric features*/

      // encode gender as 0 or 1; need to revisit this scheme
    val numericFeatures = icuEvents.map{
      f =>
        val subid = f.subject_id.toInt
        val sex: Int = {
          if (f.gender == "F")
            0
          else
            1
        }
        val hadmid: Int = {
          if (f.hadm_id.isEmpty)
            -1
          else
            f.hadm_id.toInt
        }
        val total_stay = f.icustay_total_num.toInt
        val stay_seqNum = f.subject_icustay_seq.toInt
        // mark invalid ages
        val sub_age: Double = {
          if (f.age.toDouble < 0 || f.age.toDouble > 120)
            -1.0
          else
            f.age.toDouble
        }

        // encode decision label
        val expiredInICU: Double = {
          if (f.icustay_expire_flg == "Y")
            1
          else
            0
        }

        val sapsi_f: Double = {
          if (f.sapsi_first.isEmpty)
            -1
          else
            f.sapsi_first.toDouble
        }

        val saps_min: Double = {
          if (f.sapsi_min.isEmpty)
            -1
          else
            f.sapsi_min.toDouble
        }

        val saps_max: Double = {
          if (f.sapsi_max.isEmpty)
            -1
          else
            f.sapsi_max.toDouble
        }

        // return clean and normalized data
        NormalizedPatientEvent(subid, sex, hadmid, total_stay, stay_seqNum, sub_age,
          sapsi_f, saps_min, saps_max, expiredInICU)
    }

    // Filter out bad/unnecessary data
    val filteredData = numericFeatures.filter(f => f.age >= 18).filter(f => (f.sapsi_first >= 0) && (f.sapsi_min >= 0)
     && (f.sapsi_max >= 0))

    // Normalize everything to the same scale
    // Get avgs, min, max
    val minAge = filteredData.map(f => f.age).min()
    val maxAge = filteredData.map(f => f.age).max()
    val meanAge = filteredData.map(f => f.age).mean()

    val minSAPS_first = filteredData.map(f => f.sapsi_first).min()
    val maxSAPS_first = filteredData.map(f => f.sapsi_first).max()
    val meanSAPS_first = filteredData.map(f => f.sapsi_first).mean()

    val minSAPS_min = filteredData.map(f => f.sapsi_min).min()
    val maxSAPS_min = filteredData.map(f => f.sapsi_min).max()
    val meanSAPS_min = filteredData.map(f => f.sapsi_min).mean()

    val minSAPS_max = filteredData.map(f => f.sapsi_max).min()
    val maxSAPS_max = filteredData.map(f => f.sapsi_max).max()
    val meanSAPS_max = filteredData.map(f => f.sapsi_max).mean()

    val normalizedData = filteredData.map{
      f =>
        val normedAge = (f.age - meanAge)/(maxAge - minAge)
        val normedSAPS_first = (f.sapsi_first - meanSAPS_first)/(maxSAPS_first - minSAPS_first)
        val normedSAPS_min = (f.sapsi_min - meanSAPS_min)/(maxSAPS_min - minSAPS_min)
        val normedSAPS_max = (f.sapsi_max - meanSAPS_max)/(maxSAPS_max - minSAPS_max)

        NormalizedPatientEvent(f.subject_id, f.gender, f.hadm_id,
          f.icustay_total_num, f.icustay_seq_num, normedAge, normedSAPS_first,
          normedSAPS_min, normedSAPS_max, f.icustay_expire_flg)
    }

    normalizedData
  }

  def construct_LP_for_AdmBaseline(normedPatientEvents:RDD[NormalizedPatientEvent]): RDD[LabeledPoint] = {
    /** Construct LabeledPoints for passing in to MLlib algos
      * returns RDD[LabeledPoint] = RDD[(label, (features))]*/
    val labeled = normedPatientEvents.map{
      f =>
        if (f.icustay_expire_flg == 0.0)
          LabeledPoint(0, Vectors.dense(f.age, f.gender.toDouble, f.sapsi_first))
        else
          LabeledPoint(1, Vectors.dense(f.age, f.gender.toDouble, f.sapsi_first))
    }

    labeled
  }


 def construct_LP_for_TopicADM(topicadmFeatures:RDD[(Double, Double, Int, Double, Vector)]):RDD[LabeledPoint] = {
   val labeled = topicadmFeatures.map{
     f =>
       val label = f._1
       val structVec = Seq(f._2.toDouble, f._3.toDouble, f._4.toDouble).toArray
       val unstructVec = f._5.toArray
       val completeArray = Array.concat(structVec, unstructVec)
       val featvec = Vectors.dense(completeArray)
       LabeledPoint(label, featvec)
   }

   labeled
 }

  def vectorizeNotes(icuNotes:RDD[IcuEvent]): RDD[NoteEvent] = {
    /** Appy tf-idf to identify the 500 most informative words in each patient's notes   */

    // define stopwords; list from Onix
    //  data specific stopwords appended at the end after the word 'z'
    val stopwords = Set("a", "about", "above", "across", "after", "again", "against", "all",
      "almost", "alone", "along", "already", "also", "although", "always", "among", "an",
      "and", "another", "any", "anybody", "anyone", "anything", "anywhere", "are", "area",
      "areas", "around", "as", "ask", "asked", "asking", "asks", "at", "away", "b", "back",
      "backed", "backing", "backs", "be", "became", "because", "become", "becomes", "been",
      "before", "began", "behind", "being", "beings", "best", "better", "between", "big",
      "both", "but", "by", "c", "came", "can", "cannot", "case", "cases", "certain",
      "certainly", "clear", "clearly", "come", "could", "d", "did", "differ", "different",
      "differently", "do", "does", "done", "down", "down", "downed", "downing", "downs",
      "during", "e", "each", "early", "either", "end", "ended", "ending", "ends", "enough",
      "even", "evenly", "ever", "every", "everybody", "everyone", "everything", "everywhere",
      "f", "face", "faces", "fact", "facts", "far", "felt", "few", "find", "finds", "first",
      "for", "four", "from", "full", "fully", "further", "furthered", "furthering", "furthers",
      "g", "gave", "general", "generally", "get", "gets", "give", "given", "gives", "go", "going",
      "good", "goods", "got", "great", "greater", "greatest", "group", "grouped", "grouping", "groups",
      "h", "had", "has", "have", "having", "he", "her", "here", "herself", "high", "high", "high",
      "higher", "highest", "him", "himself", "his", "how", "however", "i", "if", "important", "in",
      "interest", "interested", "interesting", "interests", "into", "is", "it", "its", "itself", "j",
      "just", "k", "keep", "keeps", "kind", "knew", "know", "known", "knows", "l", "large", "largely",
      "last", "later", "latest", "least", "less", "let", "lets", "like", "likely", "long", "longer",
      "longest", "m", "made", "make", "making", "man", "many", "may", "me", "member", "members", "men",
      "might", "more", "most", "mostly", "mr", "mrs", "much", "must", "my", "myself", "n", "necessary",
      "need", "needed", "needing", "needs", "never", "new", "new", "newer", "newest", "next", "no", "nobody",
      "non", "noone", "not", "nothing", "now", "nowhere", "number", "numbers", "o", "of", "off", "often",
      "old", "older", "oldest", "on", "once", "one", "only", "open", "opened", "opening", "opens", "or",
      "order", "ordered", "ordering", "orders", "other", "others", "our", "out", "over", "p", "part",
      "parted", "parting", "parts", "per", "perhaps", "place", "places", "point", "pointed", "pointing",
      "points", "possible", "present", "presented", "presenting", "presents", "problem", "problems",
      "put", "puts", "q", "quite", "r", "rather", "really", "right", "right", "room", "rooms", "s",
      "said", "same", "saw", "say", "says", "second", "seconds", "see", "seem", "seemed", "seeming",
      "seems", "sees", "several", "shall", "she", "should", "show", "showed", "showing", "shows",
      "side", "sides", "since", "small", "smaller", "smallest", "so", "some", "somebody", "someone",
      "something", "somewhere", "state", "states", "still", "still", "such", "sure", "t", "take",
      "taken", "than", "that", "the", "their", "them", "then", "there", "therefore", "these", "they",
      "thing", "things", "think", "thinks", "this", "those", "though", "thought", "thoughts", "three",
      "through", "thus", "to", "today", "together", "too", "took", "toward", "turn", "turned", "turning",
      "turns", "two", "u", "under", "until", "up", "upon", "us", "use", "used", "uses", "v", "very", "w",
      "want", "wanted", "wanting", "wants", "was", "way", "ways", "we", "well", "wells", "went", "were", "what",
      "when", "where", "whether", "which", "while", "who", "whole", "whose", "why", "will", "with", "within", "without",
      "work", "worked", "working", "works", "would", "x", "y", "year", "years", "yet", "you", "young", "younger",
      "youngest", "your", "yours", "z", "name", "note", "patient")

    val patientnotes = icuNotes.map(f => (f.subject_id.toInt, f.text))

    // get the patient and  each of their notes tokenized after some processing
    val tokenizednotes: RDD[(Int, Array[String])] = patientnotes.map {
      f =>
        val notext: String = f._2
        val tokenizedtext = notext.toLowerCase.split("\\s").filter(_.length > 3).filter(f => !stopwords.contains(f))
        (f._1, tokenizedtext)
    }

    // collapse all the notes for a patient into one
    val patandnotes = tokenizednotes.reduceByKey(_ ++ _)

    val hashingTF = new HashingTF()

    val hashednotes = patandnotes.map {
      f =>
        (f._1, hashingTF.transform(f._2))
    }

    val justnotes = hashednotes.map(_._2)
    justnotes.cache()
    val idf = new IDF().fit(justnotes)
    val tfidf = idf.transform(justnotes)

    // take 500 most informative words in each patient's entire set of notes
    val Patienttop500words = tfidf.map(x => x.toSparse).map { x => x.indices.zip(x.values)
      .sortBy(-_._2)
      .take(500)
      .map(_._1)
    }

    // combine the words from all patients above into one big set
    val vocabulary = Patienttop500words.collect.flatten.toSet
    val sizeVoc = vocabulary.size
    //println("vocabulary size", sizeVoc)
    //val something = Patienttop500words.map( f => f.length)
    //something.collect.foreach(println)

    // vocab dictionary (hashid, zippedID)
    val vocab: Map[Int, Int] = vocabulary.zipWithIndex.toMap
    //The following is not required unless you want to see the topic words
    //val reversevocab = vocab.map(f => (f._2, f._1))

    // This is not required unless you want to see the words in the LDA topics
    /*
    val allwords = patandnotes.flatMap(_._2).filter(f => vocab.contains(hashingTF.indexOf(f)))
    val wordmap = allwords.map{
      f =>
        val hashval = hashingTF.indexOf(f)
        (hashval, f)
    }.collectAsMap()
    */

    // generate document vectors for LDA
    val documents: RDD[(Long, Vector)] = patandnotes.map { f =>
      val counts = new mutable.HashMap[Int, Double]()
      f._2.foreach { word =>
        val hashid = hashingTF.indexOf(word)
        if (vocab.contains(hashid)) {
          val idx = vocab(hashid)
          counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
        }
      }
      (f._1.toLong, Vectors.sparse(vocab.size, counts.toSeq))
    }

    //println("THIS IS HASHMAP")
    //vocab.take(10).foreach(println)

    //println("THIS IS WORDMAP")
    //wordmap.take(10).foreach(println)

    // Set LDA parameters and build LDA model
    val numTopics = 50
    val lda = new LDA().setK(numTopics).setMaxIterations(30)
    val ldaModel = lda.run(documents)

    val avgLogLikelihood = ldaModel.asInstanceOf[DistributedLDAModel].logLikelihood / documents.count()
    val distributedLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]

    // Print topics, showing top-weighted 10 terms for each topic.
    // Not required unless you want to see topics
    /**
      * val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
      * topicIndices.foreach { case (terms, termWeights) =>
      * println("TOPIC:")
      * terms.zip(termWeights).foreach { case (term, weight) =>
      * print(s"${wordmap(reversevocab(term.toInt))}\t")
      * }
      * println()
      * }
      */


    val docinTocs = distributedLDAModel.toLocal.topicDistributions(documents)

    val patDoc = docinTocs.map(f => NoteEvent(f._1.toInt, f._2))

    patDoc
  }

}
