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
    var total = dataRDD.count().toInt
    val (newRdd, oneItemArr, countArr, oneItemCountMap) = RddUtils.removeRedundancy(sc, dataRDD, (total * minSupport).toInt)
    total = countArr.sum
    println("==== Total: " + total)
    // get the one item frequent set
    val itemsLen = oneItemArr.length
    val transLen = countArr.length
    val oneItemMap = RddUtils.getFreqOneItemset(newRdd, oneItemArr, transLen)
    newRdd.unpersist()

    println("==== oneItemMap Size: " + oneItemMap.size)
    oneItemMap.toList.foreach(x => println("Item: " + x._1 + " " + x._2.size))

    val oneItemMapBV = sc.broadcast(oneItemMap)
    val countArrBV = sc.broadcast(countArr)
    val oneItemArrBV = sc.broadcast(oneItemArr)
    val minCount = (total * minSupport).toInt

    // create the array which saves true in the size of translen
    val wholeTrans = new Array[Boolean](transLen)
    Range(0, transLen).foreach(wholeTrans(_) = true)

    // create the two items matrix and get the k-item set
    val (matrix, kItemMap) = getMatrix(sc, oneItemArrBV, oneItemMapBV, countArrBV, minCount)
//    val time = System.currentTimeMillis()
//    val (matrix, kItemMap) = getMatrix(oneItemArr, oneItemMap, countArr, total, minCount)
//    println("==== get Matrix" + (System.currentTimeMillis() - time))
    val matrixBV = sc.broadcast(matrix)

    // find the k+1 item set
    var k = 3
    while (kItemMap.size >= k) {
      val kItems = kItemMap.keys.toList
      freqItems ++= kItems
      val candidates = getAllCandidates(sc, oneItemArrBV, kItems, matrixBV, minCount, k - 1)
      checkCandidates(sc, countArrBV, minCount, candidates, kItemMap)
      candidates.unpersist()
      k += 1
    }

    // output the frequent items
    RddUtils.formattedSave(sc, outputPath, freqItems.toList, oneItemCountMap)
  }

  def getMatrix(oneItemArr: Array[String],
                oneItemMap: Map[String, Set[Int]],
                countArr: Array[Int],
                minCount: Int
               ): (mutable.HashMap[String, mutable.HashMap[String, Int]], mutable.Map[Set[String], Set[Int]]) = {
    val matrix = mutable.HashMap.empty[String, mutable.HashMap[String, Int]]
    val twoItemsMap = mutable.Map.empty[Set[String], Set[Int]]
    val len = oneItemArr.length

    for (i <- 0 until len) {
      val map = mutable.HashMap.empty[String, Int]
      for (j <- i + 1 until len) {
        val common = oneItemMap(oneItemArr(i)) & oneItemMap(oneItemArr(j))
        val count = getSupport(common, countArr)
        map.put(oneItemArr(j), count)
        if (count >= minCount) twoItemsMap.put(Set(oneItemArr(i), oneItemArr(j)), common)
      }
      matrix.put(oneItemArr(i), map)
    }

    (matrix, twoItemsMap)
  }

  def getMatrix(sc: SparkContext,
                oneItemArrBV: Broadcast[Array[String]],
                oneItemMapBV: Broadcast[Map[String, Set[Int]]],
                countArrBV: Broadcast[Array[Int]],
                minCount: Int
               ): (mutable.HashMap[String, mutable.HashMap[String, Int]], mutable.HashMap[Set[String], Set[Int]]) = {
    val matrix = mutable.HashMap.empty[String, mutable.HashMap[String, Int]]
    val twoItemsMap = mutable.HashMap.empty[Set[String], Set[Int]]
    val oneItems = oneItemArrBV.value.zipWithIndex

    val res = sc.parallelize(oneItems).map{x =>
      val (line, twoItemsMap) = getTwoCount(x._1, x._2, oneItemArrBV, oneItemMapBV, countArrBV, minCount)
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
                  oneItemMapBV: Broadcast[Map[String, Set[Int]]],
                  countArrBV: Broadcast[Array[Int]],
                  minCount: Int
                 ): (mutable.HashMap[String, Int], List[(Set[String], Set[Int])]) = {
    println("==== Enter: ")
    val oneItemArr = oneItemArrBV.value
    val oneItemMap = oneItemMapBV.value
    println("==== Finish: ")
    val countArr = countArrBV.value
    val res = mutable.HashMap.empty[String, Int]
    val twoItems = mutable.ListBuffer.empty[(Set[String], Set[Int])]

    val len = oneItemArr.size
    val size = countArr.size
    val left = oneItemMap(item)
    for (i <- index until len) {
      val right = oneItemMap(oneItemArr(i))
      val time = System.currentTimeMillis()
      val common = left & right
      val count = getSupport(common, countArr)
      res.put(oneItemArr(i), count)
      val time1 = System.currentTimeMillis() - time
      println("==== Use time: " + time1 + " " + count)
      if (count >= minCount) {
        twoItems.append((Set(item, oneItemArr(i)), common))
      }
    }

    (res, twoItems.toList)
  }


  def getSupport(common: Set[Int], countArr: Array[Int]): Int = {
    var count = 0
    common.toList.foreach(count += countArr(_))

    count
  }

  def getAllCandidates(sc: SparkContext,
                       oneItemArrBV: Broadcast[Array[String]],
                       kItems: List[Set[String]],
                       matrixBV: Broadcast[mutable.HashMap[String, mutable.HashMap[String, Int]]],
                       minCount: Int,
                       k: Int) = {
    val candidates = sc.parallelize(kItems)
      .flatMap(x => getCandidates(oneItemArrBV, x, matrixBV, minCount,k))
//      .collect()
      .distinct
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // TODO: local vs rdd

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
                      candidates: RDD[Set[String]],
                      kItemMap: mutable.Map[Set[String], Set[Int]]) = {
    val kItemMapBV = sc.broadcast(kItemMap)
    val res = candidates.map(x => checkEach(kItemMapBV, countArrBV, minCount, x))
      .filter(_._1.nonEmpty)
      .collect()
    kItemMapBV.unpersist()
    kItemMap.clear()
    kItemMap ++= res
  }

  def checkEach(kItemMapBV: Broadcast[mutable.Map[Set[String], Set[Int]]],
                countArrBV: Broadcast[Array[Int]],
                minCount: Int,
                candidate: Set[String]): (Set[String], Set[Int]) = {
    val empty = (Set.empty[String], Set.empty[Int])
    val kItemMap = kItemMapBV.value
    val countArr = countArrBV.value
    val subSets = getSubsets(candidate)
    for (s <- subSets)
      if (subSets.contains(s)) return empty
    val x = kItemMap(subSets.head)
    val y = kItemMap(subSets.last)
    val common = x & y
    val count = getSupport(common, countArr)
    if (count < minCount) return empty


    (candidate, common)
  }

  def getSubsets(candidate: Set[String]): List[Set[String]] = {
    val len = candidate.size
    val subSets = candidate.toList.map(candidate - _)

    subSets
  }

}
