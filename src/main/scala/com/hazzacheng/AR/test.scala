package com.hazzacheng.AR

import scala.collection.mutable.ArrayBuffer

object test {

  def ABC(): Unit={
    var tst01 = ArrayBuffer(0)
    var tst02 = ArrayBuffer(0)
    for (i <- 0 to 1300000) {
      tst01.append(1)
      tst02.append(i % 2)
    }
    val tst1 = System.currentTimeMillis()
    for (i <- 0 to 1300000) {
      tst01(i) & tst02(i)
    }
    print(System.currentTimeMillis() - tst1)
  }
}
