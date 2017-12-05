package com.hazzacheng.AR

import com.hazzacheng.AR.utils.RddUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.storage.StorageLevel


/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-4 
  * Time: 8:35 PM
  */
object TestMain {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val input = args(0)
    val output = args(1)
    val len = args(2).toInt
    val (dataRDD, userRDD) = RddUtils.readAsRDD(sc, input)

    val total = dataRDD.count()
    val dataRdd = dataRDD.filter(_.length < len).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val size = dataRdd.count()
    val percent = size.toDouble / total.toDouble
    println("==== Size < " + len + " :" + size + " " + percent)

    val freqItems = new FPGrowth().setMinSupport(0.092).run(dataRdd).freqItemsets.collect()
    val format = freqItems.map(x => (x.items.length, x.freq.toDouble / total.toDouble))

    println("==== FreqItems Size < " + len + " :" + format.length)

    sc.parallelize(format).saveAsTextFile(output)
  }
}
