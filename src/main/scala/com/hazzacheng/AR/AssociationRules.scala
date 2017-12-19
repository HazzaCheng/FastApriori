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

  def run(
           sc: SparkContext,
           userRDD: RDD[Array[String]]
         ) = {
    val (newRDD, empty, indexesMap) = removeRedundancy(sc, userRDD)
    println("==== Size newRDD " + newRDD.count())
    val recommends = (genAssociationRules(sc, newRDD, freqItems, indexesMap) ++ empty).sortBy(_._1)

    sc.parallelize(recommends).repartition(1).map(x => x._1 + " " + x._2).saveAsTextFile("/user_res")

    //newRDD.map(_._1.mkString(" ")).saveAsTextFile("/user_res")
  }

  def removeRedundancy(
                        sc: SparkContext,
                        userRDD: RDD[Array[String]],
                      ): (RDD[(Set[Int], Int)], Array[(Int, String)], collection.Map[Int, List[Int]]) = {
    val items = mutable.HashSet.empty[String]
    items ++= freqItems
    val itemsBV = sc.broadcast(items)
    val itemToRankBV = sc.broadcast(itemToRank)

    val temp = userRDD.zipWithIndex().map{ case (line, index) =>
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

  def genAssociationRules(
                           sc: SparkContext,
                           newRDD: RDD[(Set[Int], Int)],
                           indexesMap: collection.Map[Int, scala.List[Int]]
                         ): Array[(Int, String)] = {
    val grouped = freqItemset.groupBy(_._1.size)
    val tiem = System.currentTimeMillis()
    val subToSuper = genSuperSets(sc, grouped).sortWith(associationRulesSort).map(x => (x._1, x._2))
    println("==== Use Time sort" + (System.currentTimeMillis() - tiem))
    val subToSuperBV = sc.broadcast(subToSuper)
    val freqItemsBV = sc.broadcast(freqItems)
    val indexesMapBV = sc.broadcast(indexesMap)

    val recommends = newRDD.map { case (user, index) =>

      val time = System.currentTimeMillis()

      var recommend = -1
      val subToSuper = subToSuperBV.value
      val freqItems = freqItemsBV.value
      val indexesMap = indexesMapBV.value
      val userLen = user.size
      val filtered = subToSuper.filter(x => x._1.size <= userLen && !user.contains(x._2))

      val len = filtered.length
      var i = 0
      var flag = false
      while (!flag && i < len) {
        val tmp = filtered(i)
        if ((tmp._1 & user).size == tmp._1.size) {
          recommend = tmp._2
        }

        i += 1
      }

      subToSuper.filter(_._1.size <= userLen).foreach { case (subset, comp) =>
        val subsetLen = subset.size
        if ((subset -- user).isEmpty) {
          var flag = false
          var i = 0
          val len = comp.length
          while (!flag && i < len) {
            val tmp = comp(i)
            if (!user.contains(tmp._1)) {
              flag = true
              if (tmp._2 > max ||
                (tmp._2 == max && freqItems(tmp._1).toInt < freqItems(recommend).toInt)) {
                max = tmp._2
                recommend = tmp._1
              }
            }
            i += 1
          }
        }
      }

      println("==== Use Time " + (System.currentTimeMillis() - time) + " " + user)

      if (recommend == -1) (index, "0")
      else (index, freqItems(recommend))
    }.collect()

    subToSuperBV.unpersist()
    freqItemsBV.unpersist()
    indexesMapBV.unpersist()

    recommends
  }

  def associationRulesSort(x: (Set[Int], Int, Double), y: (Set[Int], Int, Double)): Boolean = {
    if (x._3 > y._3) true
    else if (x._3 < y._3) false
    else freqItems(x._2).toInt < freqItems(y._2).toInt
  }

  def genSuperSets(
                    sc: SparkContext,
                    grouped: Map[Int, Array[(Set[Int], Int)]]
                  ): Array[(Set[Int], Int, Double)] = {
    val minLen = grouped.keys.min
    val supersets = freqItemset.filter(_._1.size != minLen)
    val freqItemsetBV = sc.broadcast(grouped)

    val subToSuper = sc.parallelize(supersets).flatMap{ case (superset, count) =>

      val time = System.currentTimeMillis()

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

      println("==== Use Time " + (System.currentTimeMillis() - time) + " " + supersets)

      complements.toArray
    }.collect()

    freqItemsetBV.unpersist()

    subToSuper
  }


}
