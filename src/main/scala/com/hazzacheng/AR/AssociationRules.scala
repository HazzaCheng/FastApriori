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
                        private val freqItemset: Array[(Set[Int], Int)],
                        private val freqItems: Array[String],
                        private val itemToRank: mutable.HashMap[String, Int]
                      ) extends Serializable {

  def run(sc: SparkContext, userRDD: RDD[Array[String]]): Array[(Int, String)] = {
    println("==== Size userRDD " + userRDD.count())
    val (newRDD, empty, indexesMap) = removeRedundancy(sc, userRDD)
    println("==== Size newRDD " + newRDD.count())
    val freqItemsSize = freqItems.length
    val recommends = genAssociationRules(sc, newRDD, indexesMap, freqItemsSize) ++ empty

//    sc.parallelize(recommends).repartition(1).sortBy(_._1).map(x => x._1 + " " + x._2).saveAsTextFile("/user_res")

    //newRDD.map(_._1.mkString(" ")).saveAsTextFile("/user_res")
    recommends
  }

  def removeRedundancy(
                        sc: SparkContext,
                        userRDD: RDD[Array[String]]
                      ): (RDD[(Set[Int], Int)], Array[(Int, String)], collection.Map[Int, List[Int]]) = {
    val items = mutable.HashSet.empty[String]
    items ++= freqItems
    val itemsBV = sc.broadcast(items)
    val itemToRankBV = sc.broadcast(itemToRank)

    val temp = userRDD.zipWithIndex().map { case (line, index) =>
      val items = itemsBV.value
      val itemToRank = itemToRankBV.value
      val filtered = line.filter(items.contains).map(itemToRank).toSet
      (filtered, index.toInt)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val empty = temp.filter(_._1.isEmpty).map(x => (x._2, "0")).collect()
    userRDD.unpersist()
    val duplicates = temp.filter(_._1.nonEmpty)
      .map(x => (x._1, List(x._2))).reduceByKey(_ ++ _)
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val indexesMap = duplicates.map(x => (x._2.toInt, x._1._2)).collectAsMap()
    temp.unpersist()
    val newRDD = duplicates.map(x => (x._1._1, x._2.toInt)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    itemsBV.unpersist()
    itemToRankBV.unpersist()

    (newRDD, empty, indexesMap)
  }

  def genAssociationRules(sc: SparkContext,
                          newRDD: RDD[(Set[Int], Int)],
                          indexesMap: collection.Map[Int, List[Int]],
                          freqItemsSize: Int
                         ): Array[(Int, String)] = {
    val grouped = freqItemset.groupBy(_._1.size)
    val rules1 = genRules(sc, grouped)//.sortWith(associationRulesSort).map(x => (x._1, x._2, x._1.size))
    val time = System.currentTimeMillis()
    val rules = rules1.sortWith(associationRulesSort).map(x => (x._1, x._2, x._1.size))
    println("==== Use Time sort " + (System.currentTimeMillis() - time))
    println("==== Size association rules " + rules.length)
    val rulesBV = sc.broadcast(rules)
    val freqItemsBV = sc.broadcast(freqItems)
    val indexesMapBV = sc.broadcast(indexesMap)

    val recommends = newRDD.flatMap { case (user, index) =>

      val time = System.currentTimeMillis()

      var recommend = -1
      val rules = rulesBV.value
      val freqItems = freqItemsBV.value
      val indexesMap = indexesMapBV.value
      val userLen = user.size
      var time_1 = System.currentTimeMillis()

      val temp = new Array[Boolean](freqItemsSize)
      user.foreach(temp(_) = true)
      val filtered = rules.filter(x => x._3 <= userLen && !temp(x._2))

      println("==== Filtered use time: " + (System.currentTimeMillis() - time_1))

      val len = filtered.length
      var i = 0
      var flag = false
      while (!flag && i < len) {
        val tmp = filtered(i)
        if ((tmp._1 & user).size == tmp._1.size) {
          recommend = tmp._2
          flag = true
        }
        i += 1
      }
      println("==== Use Time " + (System.currentTimeMillis() - time) + " " + user + " " + filtered.length)

      if (flag) indexesMap(index).map(x => (x, freqItems(recommend)))
      else indexesMap(index).map(x => (x, "0"))
    }.collect()

    rulesBV.unpersist()
    freqItemsBV.unpersist()
    indexesMapBV.unpersist()

    recommends
  }


  def associationRulesSort(x: (Set[Int], Int, Double), y: (Set[Int], Int, Double)): Boolean = {
    if (x._3 > y._3) true
    else if (x._3 < y._3) false
    else freqItems(x._2).toInt < freqItems(y._2).toInt
  }

  def genRules(
                    sc: SparkContext,
                    grouped: Map[Int, Array[(Set[Int], Int)]]
                  ): Array[(Set[Int], Int, Double)] = {
    val supersets = freqItemset.filter(_._1.size != 1)
    val freqItemsetBV = sc.broadcast(grouped)

    val rules = sc.parallelize(supersets).flatMap { case (superset, count) =>

    //  val time = System.currentTimeMillis()

      val subsets = freqItemsetBV.value(superset.size - 1)
      val complements = mutable.ListBuffer.empty[(Set[Int], Int, Double)]
      val targets = mutable.HashSet.empty[Set[Int]]
      superset.foreach(i => targets.add(superset - i))
      var i = 0
      val subsetsLen = subsets.length
      while (targets.nonEmpty && i < subsetsLen) {
        val subset = subsets(i)
        if (targets contains subset._1) {
          complements.append((subset._1, (superset -- subset._1).head, count.toDouble / subset._2))
          targets.remove(subset._1)
        }
        i += 1
      }

      //println("==== Use Time " + (System.currentTimeMillis() - time) + " " + supersets)

      complements.toList
    }.groupBy(_._1.size).collectAsMap()

    val minLen = rules.keys.min
    val maxLen = rules.keys.max
    val realRules = mutable.ArrayBuffer.empty[(Set[Int], Int, Double)]
    realRules ++= rules(minLen)
    var lowLevel = rules(minLen).toArray
    for (i <- (minLen + 1) to maxLen) {
      val time = System.currentTimeMillis()
      val subsetsBV = sc.broadcast(lowLevel.groupBy(_._2))
      println("==== Before cut level " + i + " Nums: " + rules(i).size)
      val filtered = sc.parallelize(rules(i).toList).filter{ case (superset, recommend, confidence) =>
        val tmp = subsetsBV.value
        var flag = true
        if (tmp contains recommend) {
          val subsets = subsetsBV.value(recommend)
          val targets = mutable.HashSet.empty[Set[Int]]
          superset.foreach(i => targets.add(superset - i))
          var i = 0
          val subsetsLen = subsets.length
          while (flag && targets.nonEmpty && i < subsetsLen) {
            val subset = subsets(i)
            if (targets contains subset._1) {
              if (subset._3 >= confidence) flag = false
              targets.remove(subset._1)
            }
            i += 1
          }
          if (targets.nonEmpty) flag = false
        } else flag = false
        flag
      }.collect()
      println("==== After cut level " + i + " Nums: " + filtered.size)
      realRules ++= filtered
      lowLevel = filtered
      subsetsBV.unpersist()
      println("==== Use Time cut leaves " + i + " Time: " + (System.currentTimeMillis() - time))
    }


    freqItemsetBV.unpersist()

    realRules.toArray
  }

}
