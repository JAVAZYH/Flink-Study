package TSY.gorup.backUp

import scala.collection.mutable

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 10:23
  * \*/
object gruoptest {
  def main(args: Array[String]): Unit = {
    val a: mutable.Set[String] = mutable.Set[String]()
    a.add("111")
    a-"111"
    println(a)
  }

}