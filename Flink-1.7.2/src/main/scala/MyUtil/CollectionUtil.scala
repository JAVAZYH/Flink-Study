package MyUtil

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import java.util
object CollectionUtil {


//两个数组左连接
  def twoArrLeftJoin(arr1:Array[String],arr2:Array[String]) ={
    var result= ArrayBuffer[String]()
    for (ele1 <- arr1) {
      for (ele2 <- arr2) {
        result+=(ele1 +"|"+ ele2)
      }
    }
    result
  }

  //两个迭代器左连接
  def twoItLeftJoin(it1:util.Iterator[String],it2:util.Iterator[String]) = {
    val arr1: Array[String] = it1.toArray
    val arr2: Array[String] = it2.toArray
    val result = twoArrLeftJoin(arr1, arr2).toIterator
    result
  }


  def main(args: Array[String]): Unit = {
    val arr1= Array("a")
    val arr2= Array("1","2")

    val it1=Iterator[String]("a")
    val it2=Iterator[String]("1","2","3")

    twoItLeftJoin(it1, it2).toArray.foreach(println)

//    val result: ArrayBuffer[String] = towArrLeftJoin(arr1,arr2)
//    result.foreach(println)


  }
}
