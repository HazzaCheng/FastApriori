package com.hazzacheng.AR

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}

import scala.collection.{immutable, mutable}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-14 
  * Time: 10:03 PM
  */

class NFPGrowth(private var minSupport: Double, private var numPartitions: Int) extends Serializable {
  val nums = 4


  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  def run(
           sc: SparkContext,
           data: RDD[Array[String]]
         ): Unit/*(RDD[(Array[String], Int)], mutable.HashMap[String, Int])*/ = {
    val count = data.count()
    var minCount = math.ceil(minSupport * count).toInt
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val (freqItems, itemToRank, newData) = genFreqItems(sc, data, minCount, partitioner)
   // val newFreqItems = freqItems.indices.toArray
    val totalCount = newData.count().toInt
    data.unpersist()
    minCount = math.ceil(minSupport * totalCount).toInt
    val freqItemsets = genFreqItemsets(sc, newData, totalCount, minCount, freqItems)

//    (freqItemsets, itemToRank)
  }

  private def genFreqItems(
                            sc: SparkContext,
                            data: RDD[Array[String]],
                            minCount: Long,
                            partitioner: Partitioner
                          ):(Array[String], mutable.HashMap[String, Int], RDD[(Int, (Array[Int], Int))]) = {

    val freqItemsSet = mutable.HashSet.empty[String]
    val itemToRank = mutable.HashMap.empty[String, Int]

    val freqItems = data.flatMap(_.map((_, 1)))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
    freqItems.foreach(freqItemsSet.add)
    freqItems.zipWithIndex.foreach(x => itemToRank.put(x._1, x._2))

    val freqItemsBV = sc.broadcast(freqItemsSet)
    val itemToRankBV = sc.broadcast(itemToRank)
    val newData = data.map{x =>
      val freqItems = freqItemsBV.value
      val itemToRank = itemToRankBV.value
      (x.filter(freqItems.contains).map(itemToRank).toSet, 1)
    }.filter {
      case (transcation, count) =>
        transcation.size > 1 && transcation.size < 200
    }
      .reduceByKey(_ + _)
      .map(x => (x._1.toArray, x._2))
      .zipWithIndex()
      .map(x => (x._2.toInt, x._1))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    (freqItems, itemToRank, newData)
  }

  private def genTwoFreqItems(
                               sc: SparkContext,
                               newData: RDD[(Int, (Array[Int], Int))],
                               freqItemsTrans: Map[Int, Array[Int]],
                               totalCount: Int,
                               minCount: Int
                             ): RDD[((Int, Int), Array[Int])] = {
    val freqItemsSize = freqItemsTrans.size
    val tuples = mutable.ListBuffer.empty[(Int, Int)]
    val countMapBV = sc.broadcast(newData.map(x => (x._1, x._2._2)).collectAsMap())
    val freqItemsTransBV = sc.broadcast(freqItemsTrans)

    for (i <- 0 until freqItemsSize - 1)
      for (j <- i + 1 until freqItemsSize)
        tuples.append((i, j))

    val res = sc.parallelize(tuples.toList, sc.defaultParallelism * nums).map{t =>
      val countMap = countMapBV.value
      val freqItemsTrans = freqItemsTransBV.value
      val x = new Array[Boolean](totalCount)
      val y = new Array[Boolean](totalCount)
      freqItemsTrans(t._1).foreach(x(_) = true)
      freqItemsTrans(t._2).foreach(y(_) = true)
      val indexes = Range(0, totalCount).filter(i => x(i) && y(i)).toArray
      var count = 0
      indexes.foreach(count += countMap(_))
      if (count >= minCount) (t, indexes)
      else (t, Array.empty[Int])
    }.filter(_._2.nonEmpty)

    res
  }

  private def genFreqItemsets(
                               sc: SparkContext,
                               newData: RDD[(Int, (Array[Int], Int))],
                               totalCount: Int,
                               minCount: Int,
                               freqItems: Array[String]
                             ): Unit/*RDD[(Array[String], Int)]*/ = {
    val freqItemsTrans = getFreqItemsTrans(newData, freqItems)
    val transactionsBV = sc.broadcast(newData.collectAsMap)
    val freqItemsBV = sc.broadcast(freqItems)

    val tuples = genTwoFreqItems(sc, newData, freqItemsTrans.toMap, totalCount, minCount)
      .repartition(sc.defaultParallelism * nums * 10)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val tuplesCount = tuples.count()
    println("==== tuples " + tuplesCount)

    /*val trees = sc.parallelize(freqItemsTrans, sc.defaultParallelism * nums)
      .map(x => buildTree(x, transactionsBV))
    val len = trees.count()
    println("==== trees " + len)*/

    tuples.foreach(t => buildTree(t, transactionsBV))

  }

/*  private def genCondTransactions(
                                   transaction: Array[Int],
                                   partitioner: Partitioner
                                 ): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    var i = transaction.length - 1
    while (i >= 0) {
      val item = transaction(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) output(part) = transaction.slice(0, i + 1)
      i -= 1
    }
    output
  }*/

  private def getFreqItemsTrans(newData: RDD[(Int, (Array[Int], Int))],
                                freqItems: Array[String]
                               ): Array[(Int, Array[Int])] = {
    val itemsWithTrans = freqItems.indices
      .map(x => (x, newData.filter(y => arrayContains(y._2._1, x)).map(_._1).collect()))
      .toArray

    itemsWithTrans
  }

  private def arrayContains(transaction: Array[Int], item: Int): Boolean = {
    transaction.foreach(x => if (x == item) return true)
    false
  }

  private def buildTree(itemsWithIndexes: ((Int, Int), Array[Int]),
                        transactionsBV: Broadcast[collection.Map[Int, (Array[Int], Int)]]
                       ): Unit = {
    val time = System.currentTimeMillis()

    val items = itemsWithIndexes._1
    val indexes = itemsWithIndexes._2
    val transactions = transactionsBV.value
    val tree = new FPTree

    var sum = 0
    indexes.foreach{x =>
      val transcation = transactions(x)
      val i = findInArray(transcation._1, items)
      val path = transcation._1.slice(0, i)
      sum += path.length
//      time1 = System.nanoTime()
      tree.add(path, transcation._2)
//      println(" Add " + (System.nanoTime() - time1))
    }

    println("==== Use time: " + (System.currentTimeMillis() - time) + " " + items + " " + indexes.size + " " + sum)


  }

  private def findInArray(transaction: Array[Int], items: (Int, Int)): Int = {
    val n = transaction.length
    for (i <- 0 until n)
      if (transaction(i) == items._1 || transaction(i) == items._2) return i
    0
  }

}
