package com.hazzacheng.AR

import com.hazzacheng.AR.utils.RddUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth


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
    val (dataRDD, userRDD) = RddUtils.readAsRDD(sc, input)

    val total = dataRDD.count()
    val freqItems = new FPGrowth().setMinSupport(0.092).run(dataRDD).freqItemsets.collect()
    val format = freqItems.map(x => (x.items, x.freq / total))
    sc.parallelize(format).saveAsTextFile(output)
  }
}
