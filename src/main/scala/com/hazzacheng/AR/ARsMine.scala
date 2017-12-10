package com.hazzacheng.AR

import com.hazzacheng.AR.utils.RddUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

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
    val total = dataRDD.count().toInt
    val (newRdd, oneItemArr, countArr) = RddUtils.removeRedundancy(sc, dataRDD, (total * minSupport).toInt)
    // get the one item frequent set
    val itemsLen = oneItemArr.length
    val transLen = countArr.length
    val oneItemMap = RddUtils.getFreqOneItemset(newRdd, oneItemArr, transLen)
    newRdd.unpersist()

    val oneItemMapBV = sc.broadcast(oneItemMap)
    val countArrBV = sc.broadcast(countArr)
    val oneItemArrBV = sc.broadcast(oneItemArr)
    val minCount = (transLen * minSupport).toInt

    // create the array which saves true in the size of translen
    val wholeTrans = new Array[Boolean](transLen)
    Range(0, transLen).foreach(wholeTrans(_) = true)

    // create the two items matrix and get the k-item set
    val (matrix, kItemMap) = getMatrix(sc, oneItemArrBV, oneItemMapBV, countArrBV, total, minCount)
    val matrixBV = sc.broadcast(matrix)

    // find the k+1 item set
    var k = 2
    while (kItemMap.size >= k) {
      val candidates = getCandidates(oneItemArrBV, kItemMap.keys.toList, matrixBV, minCount, k - 1)
    }

  }

  def getMatrix(sc: SparkContext,
                oneItemArrBV: Broadcast[Array[String]],
                oneItemMapBV: Broadcast[mutable.HashMap[String, Array[Boolean]]],
                countArrBV: Broadcast[Array[Int]],
                total: Int,
                minCount: Int
               ): (mutable.HashMap[String, mutable.HashMap[String, Int]], mutable.HashMap[Set[String], Array[Boolean]]) = {
    val matrix = mutable.HashMap.empty[String, mutable.HashMap[String, Int]]
    val twoItemsMap = mutable.HashMap.empty[Set[String], Array[Boolean]]

    val res = sc.parallelize(oneItemArrBV.value.zipWithIndex).map{x =>
      val (line, twoItemsMap) = getTwoCount(x._1, x._2, oneItemArrBV, oneItemMapBV, countArrBV, total, minCount)
      ((x._1, line), twoItemsMap)
    }.collect()

    res.foreach{x =>
      twoItemsMap ++= x._2
      matrix.put(x._1._1, x._1._2)
    }

    (matrix, twoItemsMap)
  }

  def getTwoCount(item: String,
                  index: Int,
                  oneItemArrBV: Broadcast[Array[String]],
                  oneItemMapBV: Broadcast[mutable.HashMap[String, Array[Boolean]]],
                  countArrBV: Broadcast[Array[Int]],
                  total: Int,
                  minCount: Int
                 ): (mutable.HashMap[String, Int], List[(Set[String], Array[Boolean])]) = {
    val oneItemArr = oneItemArrBV.value
    val oneItemMap = oneItemMapBV.value
    val countArr = countArrBV.value
    val res = mutable.HashMap.empty[String, Int]
    val twoItems = mutable.ListBuffer.empty[(Set[String], Array[Boolean])]

    val len = oneItemArr.size
    val size = countArr.size
    val left = oneItemMap(item)
    for (i <- (index + 1 until len)) {
      val exist = new Array[Boolean](size)
      val right = oneItemMap(oneItemArr(i))
      var count = total
      var j = 0
      var flag = true
      while (flag && j < size) {
        exist(j) = left(j) && right(j)
        if (!exist(j)) count -= countArr(j)
        if (count < minCount) {
          flag = false
          res.put(oneItemArr(i), 0)
        }
        j += 1
      }
      if (flag) {
        res.put(oneItemArr(i), count)
        twoItems.append((Set(item, oneItemArr(i)), exist))
      }
    }

    (res, twoItems.toList)
  }

  def getCandidates(oneItemArrBV: Broadcast[Array[String]],
                    kItems: List[Set[String]],
                    matrixBV: Broadcast[mutable.HashMap[String, mutable.HashMap[String, Int]]],
                    minCount: Int,
                    k: Int): Unit = {

  }


}
