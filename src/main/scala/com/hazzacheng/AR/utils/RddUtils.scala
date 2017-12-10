package com.hazzacheng.AR.utils

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-5 
  * Time: 10:50 AM
  */
object RddUtils {

  def readAsRDD(sc: SparkContext,
                path: String): (RDD[Array[String]], RDD[Array[String]]) = {
    val dataRDD = sc.textFile(path + "new.dat", sc.defaultParallelism * 4).map(_.trim().split("\\s+"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val userRDD = sc.textFile(path + "U.dat", sc.defaultParallelism * 4).map(_.trim().split("\\s+"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    (dataRDD, userRDD)
  }

  def formatOutput(freqItems: Array[FPGrowth.FreqItemset[String]],
                   total: Long): Array[String] = {
    val strs = freqItems.map{x =>
      val percent = x.freq.toDouble / total
      val str = x.items.mkString(",")

      str// + " ( " + x.freq + " " + percent + " )"
    }

    strs
  }

  def removeRedundancy(sc: SparkContext,
                       data: RDD[Array[String]],
                       min: Int
                      ): (RDD[(Set[String], Long)], Array[String], Array[Int]) = {
    val itemsSet = mutable.HashSet.empty[String]
    val itemsOrders = mutable.ListBuffer.empty[(String, Int)]

    data.flatMap(_.map((_, 1)))
      .reduceByKey(_ + _)
      .filter(_._2 >= min).collect()
      .foreach { x =>
        itemsSet.add(x._1)
        itemsOrders.append((x._1, x._2))
      }
    val oneItemset = itemsOrders.toList.sortBy(_._2).map(_._1).toArray

    val size = itemsSet.size
    println("==== items count: " + size)

    val itemsBV = sc.broadcast(itemsSet)
    val nonDuplicates = data.map(x => (x.filter(itemsBV.value.contains(_)).toSet, 1))
      .filter(_._1.size > 1)
      .reduceByKey(_ + _)
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val countArr = nonDuplicates.map(x => (x._2, x._1._2)).collect().sortBy(_._1).map(_._2)
    val newRdd = nonDuplicates.map(x => (x._1._1, x._2)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    (newRdd, oneItemset, countArr)
  }

  def getFreqOneItemset(rdd: RDD[(Set[String], Long)],
                        oneItemset: Array[String],
                        size: Int): mutable.HashMap[String, Array[Int]] = {
    val oneItemMap = mutable.HashMap.empty[String, Array[Int]]
    oneItemset.foreach{x =>
      val indexes = rdd.filter(!_._1.contains(x)).map(_._2.toInt).collect()
//      val bool_Arr = mutable.ArrayBuffer.empty[Boolean]
//      for(i <- 0 until size) bool_Arr.append(true)
//      for(i <- indexes.indices) bool_Arr(indexes(i)) = false
      oneItemMap.put(x, indexes)
    }


    oneItemMap
  }

  def getTwoCandidateItemSet(oneItemSet: Array[String],
                             oneItemMap: mutable.HashMap[String, Array[Int]],
                             count: Int,
                             raw_1_Arr: Array[Boolean]):List[(Array[String], (Array[Boolean], Array[Boolean]))] = {
    val CandidateItemsInfo = mutable.ListBuffer.empty[(Array[String], (Array[Boolean], Array[Boolean]))]
    val u = oneItemSet.head
    val v = oneItemSet.last
    for(i <- oneItemSet.indices){
      for(w <- i + 1 until oneItemSet.length){
        val newItems = mutable.ArrayBuffer.empty[String]
        newItems.append(oneItemSet(i))
        newItems.append(oneItemSet(w))
        val mi = oneItemMap(oneItemSet(i))
        val i_arr = raw_1_Arr
        val mw = oneItemMap(oneItemSet(w))
        val w_arr = raw_1_Arr

        for(i <- mi.indices) i_arr(mi(i)) = false
        for(i <- mw.indices) w_arr(mi(i)) = false
        CandidateItemsInfo.append((newItems.toArray, (i_arr, w_arr)))

      }
    }
    CandidateItemsInfo.toList
  }



}
