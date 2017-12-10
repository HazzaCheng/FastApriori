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
    val itemsLen = oneItemset.length
    val transLen = countArr.length
    val oneItemMap = RddUtils.getFreqOneItemset(newRdd, oneItemset, size.toInt)
    val oneItemSetBV = sc.broadcast(oneItemset)
    val oneItemMapBV = sc.broadcast(oneItemMap)
    val res = mutable.ListBuffer.empty[Array[String]]
    val candidateItems = mutable.ListBuffer.empty[(Array[String], (Array[Boolean], Array[Boolean]))]

    // create the array which saves true in the size of translen
    val wholeTrans = new Array[Boolean](transLen)
    Range(0, transLen).foreach(wholeTrans(_) = true)

    // create the k-item set
    val kItemMap = mutable.HashMap.empty[mutable.Set[String], Array[Int]]
    oneItemMap.toList.foreach(x => kItemMap.put(mutable.Set(x._1), x._2))

    // find the k+1 item set
    var k = 2
    while (kItemMap.size >= k) {
      val candidates =
    }



  }

  def genCandidateItems(kItems: (Array[String], Array[Boolean]),
                        oneItemSetBV: Broadcast[Array[String]],
                        oneItemMapBV: Broadcast[mutable.HashMap[String, Array[Int]]],
                        raw_1_Arr: Array[Boolean]): List[(Array[String], (Array[Boolean], Array[Boolean]))] = {
    val itemSet = kItems._1.toSet
    val CandidateItems = oneItemSetBV.value.filter(!itemSet.contains(_)).map{x =>
      val CandidateItem = mutable.ArrayBuffer.empty[String]
      CandidateItem ++= kItems._1
      CandidateItem += x
      val tmp_arr = raw_1_Arr
      for (i <- oneItemMapBV.value(x).indices)
        tmp_arr(oneItemMapBV.value(x)(i)) = false

      (CandidateItem.toArray, (kItems._2, tmp_arr))
    }
    CandidateItems.toList
  }

  def checkCandidateItems(kItems: Array[String],
                          k_1ItemZeroArray: Array[Boolean],
                          oneItemZeroArray: Array[Boolean],
                          n: Int,
                          minSupport: Double): (Boolean, Array[String], Array[Boolean]) = {
    val new_bool_Arr = mutable.ArrayBuffer.empty[Boolean]
    var count = 0
    for(i <- 0 until n){
      new_bool_Arr.append (k_1ItemZeroArray(i) & oneItemZeroArray(i))
      if(new_bool_Arr(i)) count += 1
    }

    val bool_arr = mutable.ArrayBuffer.empty[Boolean]
    if(count < n * minSupport)(false, kItems, bool_arr.toArray)
    else (true, kItems, new_bool_Arr.toArray)
  }
}
