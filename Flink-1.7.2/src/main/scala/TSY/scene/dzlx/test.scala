package TSY.scene.dzlx

import java.util

import redis.clients.jedis.Jedis

import scala.collection.immutable.TreeMap
import scala.collection.{SortedMap, mutable}

import scala.collection.mutable.ArrayBuffer

class testC(val name:String,val age:Int) extends Ordered[testC] {
  override def compare(that: testC): Int = {
    val diff=this.age - that.age
    if(diff>0){
      -1
    }
    else if(diff==0){0}

    else
    {1
    }
  }

  override def toString: String = {
    this.name+":"+this.age
  }

}


//通过自定义类重写compare的方法，来实现课比较的规则
class Person(val name:String,val age:Int,val fv:Int) extends Ordered[Person] with Serializable{
  override def compare(that: Person): Int = {
    if(this.fv == that.fv){
      this.age - that.age
    }else{
      that.fv - this.fv
    }
  }

  override def toString: String = {
    this.name+"|"+this.age
  }
}



object test {
  def main(args: Array[String]): Unit = {
//    val set: mutable.Set[String] = mutable.Set[String]("英雄联盟->举报问题","英雄联盟->举报问题->常规举报问题")
//    var flag=false
//    import scala.util.control.Breaks._
//    val path="英雄联盟->举报问题->常规举报问题"
//
//      for (elem <- set) {
//        flag=path.contains(elem)
//        if(flag){
//          println(flag+elem)
//        }
//      }

//    println("2020-12-01 10:00:00".length)
//    println("2020/12/01 10:00:00".length)
//    println("20201201100000".length)
//    println("2020-12-01".length)
//    println("20201201".length)
//    println("2020/12/01".length)
//    val client: Jedis = MyUtil.ScalaUtil.RedisUtil.getJedisClient
//
//    val redisStr="CF\\xe7\\xa9\\xbf\\xe8\\xb6\\x8a\\xe7\\x81\\xab\\xe7\\xba\\xbf->\\xe5\\xb0\\x81\\xe5\\x8f\\xb7\\xe9\\x97\\xae\\xe9\\xa2\\x98->\\xe5\\xb8\\x90\\xe5\\x8f\\xb7\\xe5\\xb0\\x81\\xe5\\x8f\\xb7"
//
//    println(new String(redisStr.getBytes(), "utf-8"))

//    println("   英雄联盟->举报问题->常规举报问题".contains("CF\\xe7\\xa9\\xbf\\xe8\\xb6\\x8a\\xe7\\x81\\xab\\xe7\\xba\\xbf->\\xe5\\xb0\\x81\\xe5\\x8f\\xb7\\xe9\\x97\\xae\\xe9\\xa2\\x98->\\xe5\\xb8\\x90\\xe5\\x8f\\xb7\\xe5\\xb0\\x81\\xe5\\x8f\\xb7"))

//    println("和平精英->安全打击->封号1年及以下".contains("和平精英->安全打击->封停10年"))

//    println((0 * 1.0 / 1).toFloat)
////    println("2021-01-04 15:21:12".hashCode)
//    println("CCF->道具问题->道具异常".contains("CF->道具问题"))
//    println("CCF->道具问题->道具异常".contains("CF->道具问题"))

//    def matchPartition(input:Int) ={
//      input match{
//        case x if x>=10 && x <20=> "10-20"
//        case x if x>=20 && x <30=> "20-30"
//        case x if x>=30 && x <40=> "30-40"
//        case x if x>=40 && x <50=> "40-50"
//        case x if x>=50 && x <60=> "50-60"
//        case x if x>=60 && x <70=> "60-70"
//        case x if x>=70 && x <80=> "70-80"
//        case x if x>=80 && x <90=> "80-90"
//        case _=>"other"
//      }
//    }
//
//    println(matchPartition(23))

//    val arr=ArrayBuffer[Long](1,2,3,4)
//    println(arr(arr.length - 2))

//    def fieldIsNull(inputStr:String): Boolean ={
//      null==inputStr || inputStr=="null" || inputStr==""
//    }
//
//    val fieldIndex=1



//    object descSortMap extends Ordering[String] {
//      override def compare(input1: String, input2: String): Int = {
//        val input1Arr: Array[String] = input1.split('|')
//        val input2Arr: Array[String] = input2.split('|')
//        val value1 = if(fieldIsNull(input1Arr(fieldIndex))) 0 else input1Arr(fieldIndex).toInt
//        val value2 = if(fieldIsNull(input2Arr(fieldIndex))) 0 else input2Arr(fieldIndex).toInt
//        value2-value1
//      }
//    }
//
//    var testMap: SortedMap[String, String] = SortedMap[String,String]()(descSortMap)
//    testMap=testMap+("a"->"2","e"->"8")
//    testMap=testMap+("f"->"4")
//    println(testMap.mkString("|"))

//    val a=null
//    println(a.isInstanceOf[Int])
//    println(2000000 / 1000)

//}
//    var map1: TreeMap[Double, String] = new TreeMap[Double,String]()(descOrdering)
//    map1+=(0.3->"d")
//    map1+=(0.7->"e")
//    map1+=(0.1->"f")
//    map1+=(0.7->"e")
//
//    println(map1.mkString("|"))
//
//
//
//    val map2 = new util.TreeMap[Double,String]()
//    map2.put(0.3,"d")
//    map2.put(0.4,"a")
//    map2.put(1,"c")
//    map2.put(1,"f")
//    import  scala.collection.JavaConverters._
//    println(map2.entrySet().iterator().asScala.toList.mkString("|"))





//    if(map2.containsKey(0.4)){
//      map2.remove(0.4)
//      map2.put(0.4,"eee")
//      println(map2.headMap(0.4).size())
//    }
//
//
//    println(map2.headMap(0.1).size())
//
//    println(map2.entrySet().iterator().asScala.toList.mkString("|"))



//    println(map1.mkString("|"))

//    val l1 = List(20,30,50,60,70)
//    val l2 = List(20,30,40,50,60)
//    println(l1.equals(l2))
//    println(l2.diff(l1))

//    object descOrdering extends Ordering[Double] {
//      override def compare(x: Double, y: Double): Int = {
//        if (x > y) {
//          -1
//        }
//        else if (x == y) {
//          1
//        }
//        else {
//          1
//        }
//      }
//    }

//      var map = new TreeMap[Double,List[String]]()
//        map+=(20.3->List("aaa"))
//        map+=(20.3->List("bbb"))
//        map+=(20.3->"sdf")
//    println(map.mkString("|"))
//    println(map.get(20.3))
//    println(map.get(20.3).get)




//    val arr: ArrayBuffer[testC] = ArrayBuffer[testC]()
//    val a1 = new testC("a",60)
//    val a2 = new testC("a",10)
//    val a3 = new testC("a",5)
//    arr.append(a1)
//    arr.append(a2)
//    arr.append(a3)
//    arr.append(a3)
//
//    println(arr.mkString("|"))
//
//
//    val pairs = Array(
//      ("a", 10),
//      ("c", 5),
//      ("e", 5),
//      ("b",60),
//      ("b",6)
//    )
//
//    //按第三个字段升序，第一个字段降序，注意，排序的字段必须和后面的tuple对应
//    val bx= pairs.
//      sortBy(r => (r._2, r._1))( Ordering.Tuple2(Ordering.Int, Ordering.String) )
//    //打印结果
//
//    bx.map( println )

//    val arr1: ArrayBuffer[(Int, String)] = ArrayBuffer((23,"a"),(24,"a"),(24,"b"))
    var arr2: ArrayBuffer[(Int, String)] = ArrayBuffer((25,"a"),(20,"a"),(24,"b"),(29,"5"))
    val arr4: ArrayBuffer[(Int, String)] = arr2.sorted(Ordering.Tuple2(Ordering.Int.reverse,Ordering.String))
    println(arr4)
//    arr2 = arr2.sortWith{
//      case (a,b)=> {
//        if (a._1 == b._1) {
//          a._2<b._2
//        }
//        else {
//          a._1 > b._1
//        }
//      }
//    }
//        arr2.map(println)


    println(arr2.mkString("|"))

//    println(arr2.diff(arr1).head)
//    println(arr2.indexOf(arr2.diff(arr1).head))
//    println(arr1.equals(arr2))


//    val arr3: ArrayBuffer[(Int, String)] = ArrayBuffer()
//    val newarr: ArrayBuffer[(Int, String)] = arr3.clone()
//    arr3.append(1->"s")
//    println(newarr.mkString("|"))
//    val arr4: ArrayBuffer[(Int, String)] = ArrayBuffer((23,"a"),(24,"a"),(24,"b"))
//    val arr4: ArrayBuffer[(Int, String)] = ArrayBuffer((23,"a"),(24,"a"),(24,"b"))


  }

}
