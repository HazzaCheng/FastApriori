package com.hazzacheng.AR

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.AssociationRules
object Match {
    def match_U(sc: SparkContext,data: RDD[String], model: FPGrowthModel[String], minConfidence: Double,rjk : RDD[AssociationRules.Rule[String]]): RDD[(AssociationRules.Rule[String], Float)] ={
    val tu = data.map(x => x.split(" "))

      println("partx6")
    val t= tu.collect()                     //braodcast
    val broad_t =  sc.broadcast(t)

    val pre = broad_t.value.flatten
    val conse = rjk.map(rule =>(rule,rule.consequent))
      .filter(rule => !pre.contains(rule._2.mkString("")))
      .map(x=>x._1)


    val ru = conse.filter{rule => {
         broad_t.value.contains(rule.antecedent)
      }}


      println("partx7")
    val result = cala(t,ru)
      println("partx8")

    result
  }

  def cala(t: Array[Array[String]], ru: RDD[AssociationRules.Rule[String]]):  RDD[(AssociationRules.Rule[String], Float)] = {
    val t_set = t.map(line => line.toSet)
    val countPj = ru.map ( rule => (rule, calConf(rule, t_set)))
    countPj
  }

  def calConf(rule: AssociationRules.Rule[String], tu: Array[Set[String]]): Float = {
    val whole = (rule.antecedent.toSet + rule.consequent.toSet).map(x=>x.toString)      //

    val anteCounter = tu.count(x => (rule.antecedent.toSet & x).nonEmpty)
    val wholeCounter = tu.count(x => (whole & x).nonEmpty)

    anteCounter.toFloat / wholeCounter
  }
}
