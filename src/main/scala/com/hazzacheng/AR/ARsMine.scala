package com.hazzacheng.AR

import com.hazzacheng.AR.utils.RddUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
                  outputPath: String,
                  dataRDD: RDD[Array[String]],
                  userRDD: RDD[Array[String]],
                  minSupport: Double) = {
    val freqItems = mutable.ListBuffer.empty[Set[String]]
    // remove the items whose frequency is fewer than minimum support
    val total = dataRDD.count().toInt
    val (newRdd, oneItemArr, countArr, oneItemCountMap) = RddUtils.removeRedundancy(sc, dataRDD, (total * minSupport).toInt)
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
      val kItems = kItemMap.keys.toList
      freqItems ++= kItems
      val candidates = getAllCandidates(sc, oneItemArrBV, kItems, matrixBV, minCount, k - 1)
      checkCandidates(sc, countArrBV, minCount, total, transLen, candidates, kItemMap)
      candidates.unpersist()
    }

    // output the frequent items
    RddUtils.formattedSave(sc, outputPath, freqItems.toList, oneItemCountMap)
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
    for (i <- index until len) {
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

  def getAllCandidates(sc: SparkContext,
                       oneItemArrBV: Broadcast[Array[String]],
                       kItems: List[Set[String]],
                       matrixBV: Broadcast[mutable.HashMap[String, mutable.HashMap[String, Int]]],
                       minCount: Int,
                       k: Int) = {
    val candidates = sc.parallelize(kItems)
      .flatMap(x => getCandidates(oneItemArrBV, x, matrixBV, minCount, k))
//      .collect()
      .distinct
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    candidates
  }

  def getCandidates(oneItemArrBV: Broadcast[Array[String]],
                    items: Set[String],
                    matrixBV: Broadcast[mutable.HashMap[String, mutable.HashMap[String, Int]]],
                    minCount: Int,
                    k: Int): List[Set[String]] = {
    val oneItemArr = oneItemArrBV.value
    val matrix = matrixBV.value
    val iFirst = findIFirst(oneItemArr, items)
    val iLast = findILast(oneItemArr, items)
    val res = mutable.ListBuffer.empty[Set[String]]

    for (i <- 0 until iLast) {
      val vw = matrix(oneItemArr(iLast))
        .getOrElse(oneItemArr(i), matrix(oneItemArr(i)).getOrElse(oneItemArr(iLast), 0))
      val uw = matrix(oneItemArr(iFirst))
        .getOrElse(oneItemArr(i), matrix(oneItemArr(i)).getOrElse(oneItemArr(iFirst), 0))
      if (vw >= minCount && uw >= minCount && vw >= k && !items.contains(oneItemArr(i)))
        res.append(items + oneItemArr(i))
    }

    res.toList
  }


  def findIFirst(oneItemArr: Array[String], items: Set[String]): Int = {
    val len = oneItemArr.length
    for (i <- 0 until len)
      if (items contains oneItemArr(i)) return i
    0
  }

  def findILast(oneItemArr: Array[String], items: Set[String]): Int = {
    val len = oneItemArr.length
    for (i <- 0 until len)
      if (items contains oneItemArr(len - 1 - i)) return len - 1 - i
    len - 1
  }

  def checkCandidates(sc: SparkContext,
                      countArrBV: Broadcast[Array[Int]],
                      minCount: Int,
                      total: Int,
                      translen: Int,
                      candidates: RDD[Set[String]],
                      kItemMap: mutable.HashMap[Set[String], Array[Boolean]]) = {
    val kItemMapBV = sc.broadcast(kItemMap)
    val res = candidates.map(x => checkEach(kItemMapBV, countArrBV, minCount, total, translen, x))
      .filter(_._1.nonEmpty)
      .collect()
    kItemMapBV.unpersist()
    kItemMap.clear()
    kItemMap ++= res
  }

  def checkEach(kItemMapBV: Broadcast[mutable.HashMap[Set[String], Array[Boolean]]],
                countArrBV: Broadcast[Array[Int]],
                minCount: Int,
                total: Int,
                transLen: Int,
                candidate: Set[String]): (Set[String], Array[Boolean]) = {
    val kItemMap = kItemMapBV.value
    val countArr = countArrBV.value
    val subSets = getSubsets(candidate)
    for (s <- subSets)
      if (subSets.contains(s)) return (Set.empty[String], Array.empty[Boolean])
    val x = kItemMap(subSets.head)
    val y = kItemMap(subSets.last)
    var count = total
    val exist = new Array[Boolean](transLen)
    for (i <- 0 until transLen) {
      exist(i) = x(i) && y(i)
      if (!exist(i)) count -= countArr(i)
      return (Set.empty[String], Array.empty[Boolean])
    }

    (candidate, exist)
  }

  def getSubsets(candidate: Set[String]): List[Set[String]] = {
    val len = candidate.size
    val subSets = candidate.toList.map(candidate - _)

    subSets
  }

}
