/**
  * Created by Administrator on 2017/12/10.
  */
import org.scalatest.FunSuite
import scala.collection.mutable

class usualTest extends FunSuite{
  test("1"){
    val freqItems = Array("125","146","852","26")
    def associationRulesSort(x: (Set[Int], Int, Double), y: (Set[Int], Int, Double)): Boolean = {
      if (x._3 > y._3) true
      else if (x._3 < y._3) false
      else freqItems(x._2).toInt < freqItems(y._2).toInt
    }
    val data = Array((Set(2,3),1,0.55),(Set(1,3),2,0.6),(Set(4,5),3,0.55))
    data.toList.sortWith(associationRulesSort).foreach(println)
  }
  test("2"){
    def test1(): Unit ={
      val data = mutable.ArrayBuffer.empty[(Set[Int], Int)]
      for(i <- 0 to 1000000){
        data.append((Set(i), i))
      }
      val time = System.currentTimeMillis()
      data.toArray.filter(x => x._1.size > 5 && x._2 == 3)
      println(System.currentTimeMillis() - time)
    }
  }
}
