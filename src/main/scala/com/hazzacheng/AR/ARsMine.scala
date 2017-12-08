package com.hazzacheng.AR

import com.hazzacheng.AR.utils.RddUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
    val oneItemMap = RddUtils.getFreqOneItemset(newRdd, oneItemset, transLen)

  }
}
