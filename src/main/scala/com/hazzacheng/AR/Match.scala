package com.hazzacheng.AR

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.AssociationRules
object Match {
    def match_U(data: RDD[String], model: FPGrowthModel[String], minConfidence: Double,rjk : RDD[AssociationRules.Rule[String]]): RDD[(AssociationRules.Rule[String], Float)] ={
    //val data = sc.textFile("data/U.dat")
    val tu = data.map(x => x.split(" "))

    val t= tu.collect()                     //braodcast
    val ru = rjk.filter{rule => {
         t.contains(rule.antecedent) && !t.contains(rule.consequent)
      }}

    val result = cala(t,ru)
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
