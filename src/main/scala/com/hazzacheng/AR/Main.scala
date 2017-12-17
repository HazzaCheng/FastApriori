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
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("spark.rdd.compress", "true")
      .set("spark.speculation", "true")
      .set("spark.storage.memoryFraction", "0.3")
      .set("spark.default.parallelism", "180")
      .set("spark.shuffle.compress", "true")

    val sc = new SparkContext(conf)
    val minSupport = 0.092
    val input = args(0)
    val output = args(1)
    val (dataRDD, userRDD) = utils.RddUtils.readAsRDD(sc, input)
//    val (freqItemsets, itemToRank) =
//    new NFPGrowth(minSupport).run(sc, dataRDD)
    new Apriori(minSupport, 180).run(sc, dataRDD, output)
//    utils.RddUtils.formattedSave(sc, output, freqItemsets, itemToRank)
  }
}

