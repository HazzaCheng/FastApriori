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
import org.apache.spark.mllib.fpm.FPGrowth
import java.io.{File, PrintWriter}

import org.apache.spark.mllib.linalg.Vectors

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FPGrowthTest")
    //.set("spark.sql.warehouse.dir", "~/ideaWorkspace/ScalaSparkMl/spark-warehouse")
    val sc = new SparkContext(conf)


    val minSupport = 0.092
    val minConfidence = 0.8
    //val sumCores = conf.getInt("spark.executor.cores", 4) * conf.getInt("spark.executor.instances", 12)
    //val partitioner = new HashPartitioner(sumCores * 2)
    val sumCores = sc.defaultParallelism * 4
    println(sumCores)
    //Apriori.run(sc, args(0), args(1)@Since("1.3.0")
    //println("finish")

    //val data = sc.textFile("data/sample_fpgrowth.txt")
    //val data_D = sc.textFile(args(0), sc.defaultParallelism *4)
    //val data_U = sc.textFile(args(1), sc.defaultParallelism *4)
    val data = sc.wholeTextFiles(args(0), sc.defaultParallelism * 4)
    val filename = data.keys.collect()
    //val data_D = sc.textFile(,sc.defaultParallelism*4)
    val data_D = data.filter(file => file._1.equals(filename(0))).values
    val data_U = data.filter(file => file._1.equals(filename(1))).values
    println("part2")

    Kmeans.kmeans(data_D,sumCores,args(1))
    println("part3")


    //val dataset = data_D.map(line => line.split('\n')).flatMap(item=>item).repartition(sumCores)
    //val transactions = dataset.map(x => x.split(" ")).cache()

    //    val total = data_D.map(x => x.split(" ")).map(x=>(x.length,x))
//    val transactions = total.groupByKey().cache()
//
//    transactions.map(x=> fprun(x,sc))
//    print(transactions.partitions.mkString(" "))


    //val temp = transactions.collect()
    //    val dataSize = data_D.count()
    //    val minSupport = (dataSize * supportThreshold).toLong
    //        val temp = transactions.collect()
    //        for (i <- temp)
    //          println(i)

    /*
        val fpg = new FPGrowth()
          .setMinSupport(minSupport)
          .setNumPartitions(sumCores)
        println("xxxxx")

        val model = fpg.run(transactions)

        println("yyyyyyyy")

        //查看所有的频繁项集，并且列出它出现的次数
        model.freqItemsets.collect().foreach(itemset => {
          println(itemset.items.mkString("[", ",", "]") + "," + itemset.freq)
        })

        val rjk = model.generateAssociationRules(minConfidence)

        println("part3")

        val result = Match.match_U(data_U, model, minConfidence, rjk).map(
          rule => (rule._1.antecedent, List((rule._1.consequent, rule._2))))
          .reduceByKey(
            _ ::: _)
          .map(
            item => (item._1, item._2.sortWith(_._2 > _._2))
          ).foreach(
          res => println(res._1.mkString(",") + "-----" + res._2.head._1 + "------" + res._2.head._2)
        )
    */

    //通过置信度筛选出推荐规则则
    //antecedent表示前项
    //consequent表示后项
    //confidence表示规则的置信度
    //model.generateAssociationRules(minConfidence).collect().foreach(rule => {
    //  println(rule.antecedent.mkString(",") + "-->" +
    //    rule.consequent.mkString(",") + "-->" + rule.confidence)
    //})

    //println(model.freqItemsets
    // .filter( rule => rule.items.length !=1)
    // .collect().map(item => item.items.length).sum)

    //查看规则生成的数量
    //println(model.generateAssociationRules(minConfidence).collect().length)


    //并且所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重

    //    println("part3")
    //    val patterns = ParallelFPGrowth(sc, data_D, minSupport, " ", 5)
    //    // Print results on the terminal.
    //    println("part4")
    //    var count: Int = 0
    //    for (pattern <- patterns.collect) {
    //      println(pattern._1 + " " + pattern._2)
    //      count += 1
    //    }
    //    println("---------------------------------------------------------")
    //    println("count = " + count)
    //    println("---------------------------------------------------------")
    //
    //    //Write elements of patterns as text files in a given directory in hdfs.
    //    patterns.saveAsTextFile(args(1))

    // =============parallize================

//    val firdd = transactions.mapPartitions(p => fprun(p, sc))
//
//    val bro_firdd = sc.broadcast(firdd)
//
//    val supp_item = firdd.mapPartitions(p => getFrequet(p,sc))
  }

//  def fprun[T](iter: Iterator[T], sc: SparkContext): Iterator[(T, T)] = {
//    val minSupport = 0.092
//    val minConfidence = 0.8
//    val sumCores = 6
//    val fpg = new FPGrowth()
//      .setMinSupport(minSupport)
//      .setNumPartitions(sumCores)
//    var res = List[(T, T)]()
//    var model = fpg.run(sc.parallelize(iter.toList))
//    res.iterator
//  }

//
//
//  def getFrequet[T](iter: Iterator[T], sc: SparkContext): Iterator[(T, T)] = {
//    val minSupport = 0.092
//    val minConfidence = 0.8
//    val sumCores = 6
//    val fpg = new FPGrowth()
//      .setMinSupport(minSupport)
//      .setNumPartitions(sumCores)
//    var res = List[(T, T)]()
//    var model = fpg.run(sc.parallelize(iter.toList))
//    res.iterator
//  }


}

