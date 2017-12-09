package com.hazzacheng.AR

import com.hazzacheng.AR.utils.RddUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-8 
  * Time: 9:24 PM
  */
object ARsMine {
  def findOnSpark(sc: SparkContext,
                  dataRDD: RDD[Array[String]],
                  userRDD: RDD[Array[String]],
                  minSupport: Double): Unit = {
    // remove the items whose frequency is fewer than minimum support
    val size = dataRDD.count()
    val (newRdd, oneItemset, countArr) = RddUtils.removeRedundancy(sc, dataRDD, (size * minSupport).toInt)
    // get the one item frequent set
    // get the one item frequent set
    val itemsLen = oneItemset.length
    val transLen = countArr.length
    val oneItemMap = RddUtils.getFreqOneItemset(newRdd, oneItemset, transLen)
    val oneItemSetBV = sc.broadcast(oneItemset)
    val oneItemMapBV = sc.broadcast(oneItemMap)
    val res = mutable.ListBuffer.empty[Array[String]]
    val CandidateItem = mutable.ListBuffer.empty[(Array[String], (Array[Int], Array[Int]))]
    CandidateItem ++= RddUtils.getTwoCandidateItemSet(oneItemset, oneItemMap, (size * minSupport).toInt)
    while(CandidateItem.toList.nonEmpty){
      val candidatesRDD = sc.parallelize(CandidateItem.toList, sc.defaultParallelism * 4)
      CandidateItem.clear()
      val kFreqItemsInfo = candidatesRDD.map(x => checkCandidateItems(x._1, x._2._1, x._2._2, size.toInt, minSupport)).filter(_._1).map(x => (x._2, x._3)).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val kFreqInfo = kFreqItemsInfo.collect()
      res ++= kFreqInfo.map(_._1)
      val tempItemsInfo = kFreqItemsInfo.flatMap(x => genCandidateItems(x, oneItemSetBV, oneItemMapBV)).collect().toList
      val dict = tempItemsInfo.map(x => x._1).distinct.toSet
      if(dict.size >= dict.head.length + 1)CandidateItem ++= tempItemsInfo.filter(x => dict.contains(x._1))
    }

  }

  def genCandidateItems(kItems: (Array[String], Array[Int]),
                        oneItemSetBV: Broadcast[Array[String]],
                        oneItemMapBV: Broadcast[mutable.HashMap[String, Array[Int]]]): List[(Array[String], (Array[Int], Array[Int]))] = {
    val itemSet = kItems._1.toSet
    val CandidateItems = oneItemSetBV.value.filter(!itemSet.contains(_)).map{x =>
      val CandidateItem = mutable.ArrayBuffer.empty[String]
      CandidateItem ++= kItems._1
      CandidateItem += x
      (CandidateItem.toArray, (kItems._2, oneItemMapBV.value(x)))
    }
    CandidateItems.toList
  }

  def checkCandidateItems(kItems: Array[String],
                          k_1ItemZeroArray: Array[Int],
                          oneItemZeroArray: Array[Int],
                          n: Int,
                          minSupport: Double): (Boolean, Array[String], Array[Int]) = {
    val ans = k_1ItemZeroArray.toSet ++ oneItemZeroArray.toSet
    val i_arr = mutable.ArrayBuffer.empty[Int]
    if(ans.size < n * minSupport)(false, kItems, i_arr.toArray)
    else (true, kItems, ans.toArray)
  }
}
