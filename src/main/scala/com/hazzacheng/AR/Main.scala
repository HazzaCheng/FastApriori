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
import org.apache.spark.mllib.fpm.FPGrowth

object Main {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FPGrowthTest")
    //.set("spark.sql.warehouse.dir", "~/ideaWorkspace/ScalaSparkMl/spark-warehouse")
    val sc = new SparkContext(conf)


    val minSupport = 0.092
    val minConfidence = 0.8
    val numPartitions = sc.defaultParallelism*4

    println("part1")
    //Apriori.run(sc,"data/sample_fpgrowth.txt","out",minSupport)
    //val data = sc.textFile("data/sample_fpgrowth.txt")
    val data_D = sc.textFile(args(0), sc.defaultParallelism *4)
    val data_U = sc.textFile(args(1), sc.defaultParallelism *4)
    val transactions = data_D.map(x => x.split(" "))
    transactions.cache()
    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)

    val model = fpg.run(transactions)


    //查看所有的频繁项集，并且列出它出现的次数
    model.freqItemsets.collect().foreach(itemset => {
      println(itemset.items.mkString("[", ",", "]") + "," + itemset.freq)
    })

    val rjk = model.generateAssociationRules(minConfidence)

    println("part2")

    val result = Match.match_U(data_U,model,minConfidence,rjk).map(
      rule => (rule._1.antecedent , List((rule._1.consequent,rule._2))))
      .reduceByKey(
        _:::_)
      .map(
        item => (item._1, item._2.sortWith(_._2 > _._2))
      ).foreach(
      res => println(res._1.mkString(",") + "-----" + res._2.head._1 + "------" + res._2.head._2)
    )


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

  }
}