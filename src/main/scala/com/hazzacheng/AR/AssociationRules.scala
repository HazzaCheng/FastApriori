package com.hazzacheng.AR

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-18 
  * Time: 9:01 PM
  */
class AssociationRules(
                        private val freqItemset: Array[(Set[Int], Int)]
                      ) extends Serializable {

  def run(
           sc: SparkContext,
           userRDD: RDD[Array[String]],
           itemToRank: mutable.HashMap[String, Int],
           freqItems: Array[String]
         ) = {

  }

  def removeRedundancy(
                      sc: SparkContext
                      ) = {

  }

}
