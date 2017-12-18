package com.hazzacheng.AR

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
    val (newRDD, empty) = removeRedundancy(sc, userRDD, itemToRank, freqItems)

    newRDD.map(_._1.mkString(" ")).saveAsTextFile("/user_res")
  }

  def removeRedundancy(
                        sc: SparkContext,
                        userRDD: RDD[Array[String]],
                        itemToRank: mutable.HashMap[String, Int],
                        freqItems: Array[String]
                      ): (RDD[(Set[Int], Int)], Array[(Int, Int)]) = {
    val items = mutable.HashSet.empty[String]
    items ++= freqItems
    val itemsBV = sc.broadcast(items)
    val itemToRankBV = sc.broadcast(itemToRank)

    val temp = userRDD.zipWithIndex().map{ case (line, index) =>
      val items = itemsBV.value
      val itemToRank = itemToRankBV.value
      val filtered = line.filter(items.contains).map(itemToRank).toSet
      (filtered, index.toInt)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val empty = temp.filter(_._1.isEmpty).map(x => (x._2, 0)).collect()
    val newRDD = temp.filter(_._1.nonEmpty).persist(StorageLevel.MEMORY_AND_DISK_SER)

    itemsBV.unpersist()
    itemToRankBV.unpersist()

    (newRDD, empty)
  }

}
