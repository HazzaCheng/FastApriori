package com.hazzacheng.AR

import com.hazzacheng.AR.utils.RddUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.apache.spark.broadcast.Broadcast

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
    val CandidateItem = mutable.ListBuffer.empty[(Array[String], (Array[Int], Array[Int]))]
    CandidateItem ++= RddUtils.getTwoCandidateItemSet(oneItemset, oneItemMap)
    while(CandidateItem.toList.nonEmpty){
      val candidatesRDD = sc.parallelize(CandidateItem.toList, sc.defaultParallelism * 4)
      CandidateItem.clear()
      val kFreqItemsInfo = candidatesRDD.map(x => checkCandidateItems(x._1, x._2._1, x._2._2)).filter(_._1).map(x => (x._2, x._3))
      CandidateItem ++= kFreqItemsInfo.map(x => genCandidateItems(x, oneItemSetBV, oneItemMapBV)).collect().toList.distinct
    }
  }

  def genCandidateItems(kItems: (Array[String], Array[Int]),
                        oneItemSetBV: Broadcast[Array[String]],
                        oneItemMapBV: Broadcast[mutable.HashMap[String, Array[Int]]]): (Array[String], (Array[Int], Array[Int])) = {


  }

  def checkCandidateItems(kItems: Array[String],
                          k_1ItemZeroArray: Array[Int],
                          oneItemZeroArray: Array[Int]): (Boolean, Array[String], Array[Int]) = {

  }
}
