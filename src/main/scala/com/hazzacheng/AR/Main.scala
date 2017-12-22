package com.hazzacheng.AR

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-09-26
  * Time: 10:04 PM
  */
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("spark.default.parallelism", "300")
      .set("spark.speculation", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.getAll.foreach(x => println("==== " + x))

    val sc = new SparkContext(conf)
    val minSupport = 0.092
    val input = args(0)
    val output = args(1)
    val (dataRDD, userRDD) = Utils.readAsRDD(sc, input)

    val time1 = System.currentTimeMillis()
    val (freqItemsets, itemToRank, freqItems) =
      new FastApriori(minSupport, sc.defaultParallelism).run(sc, dataRDD)
    Utils.saveFreqItemset(sc, output, freqItemsets, freqItems)
    println("==== Total time for get freqItemsets " + (System.currentTimeMillis() - time1))

    val time2 = System.currentTimeMillis()
    val recommends = new AssociationRules(freqItemsets, freqItems, itemToRank).run(sc, userRDD)
    Utils.saveRecommends(sc, output, recommends)
    println("==== Total time for get recommends " + (System.currentTimeMillis() - time2))
  }
}

