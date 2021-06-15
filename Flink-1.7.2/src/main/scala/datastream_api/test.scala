package datastream_api

import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import MyUtil.TimeUtil
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.types.Row

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.macros.ParseException
import scala.collection._
import scala.util.Random


object test {
  def main(args: Array[String]): Unit = {

//    StreamingExecution
//    println("2020-02-03 12:23:32".toLong)
//    var row1=new Row(5)
//    var row2=new Row(1)
//    row1.setField(0,1)
//    row1.setField(1,"a")
//    row1.setField(2,"a")
//    row1.setField(3,"a")
//    row1.setField(4,"a")
//    row2.setField(0,2)
//  val result: String = row1.toString+','+row2.toString
//    def stringToRow(str:String) ={
//      val arr: Array[String] = str.split(',')
//      val result=new Row(arr.length)
//      for(ele <- arr.indices){
//        result.setField(ele,arr(ele))
//      }
//      result
//    }
//def resultRow(row:Row): Row ={
//  val result=new Row(1)
//  var str=""
//  for(ele <- 0 until  row.getArity){
//    if(ele==0){
//      str += row.getField(ele)
//    }
//    else{
//      str+="|"+row.getField(ele)
//    }
//  }
//  result.setField(0,str)
//  result
//}
//
//    val result = resultRow(row1)



//    println(result)
//    val a=10L
//    val b: String = a.asInstanceOf[String]
//    println(a.toString)
//    println(b)

//      val str="a    "
//      println(str.trim)

    //向可变set集合中添加元素
//    var test=mutable.Set[String]()
//    test+="1"
//    test.foreach(println)

    //获取当前时间戳今夜12点的时间
//    val ts: Long = System.currentTimeMillis()
//    val day = ts / (1000 * 60 * 60 * 24 ) + 1
//    // 获取定时器下一次计算时间
//    val timer = day * (1000 * 60 * 60 * 24 )-(1000*60*60*8)
//    println(TimeUtil.timestampToString(ts))
//    println(TimeUtil.timestampToString(timer))

    //将可变集合Set中的数据拼接为一个字符串
//        var test=mutable.Set[String]("a","b")
//        val arr: Array[String] = test.toArray
//        var str=""
//        for(ele <- arr.indices){
//          if (ele==0){
//            str+=arr(ele)
//          }else{
//            str+="|||"+arr(ele)
//          }
//        }
//    println(str)



    //List转为tuple
//    def listToTuple[A <: Object](list:List[A]) = {
//      val class2 = Class.forName("scala.Tuple" + list.size)
//      class2.getConstructors.apply(0).newInstance(list:_*)
//    }

//    val x=List(1,2,3,4)
//    val t = x match {
//      case List(a, b, c, _*) =>
//        (a, b)
//
//    }
//    println(listToTuple(List("Scala", "Smart","6666")))

    //定时器测试
//    val ts: Long = System.currentTimeMillis()
//    val tmp=ts/(1000*60)
//    val day = ts / (1000 * 60 * 60 * 24 ) + 1
//    // 获取定时器下一次计算时间，-8是因为时区问题
//    val timer = day * (1000 * 60 * 60 * 24 )-((1000*60*60*8)+(1000*60*1))
//    println(TimeUtil.timestampToString(day * (1000 * 60 * 60 * 24 )))
//    println(TimeUtil.timestampToString(ts))
//    println(TimeUtil.timestampToString(timer))
//    println(TimeUtil.stringToTimestamp("2020-07-10 23:59:00"))
//    println(timer)

    //去除字符传中-字符
//    val str = "2020-07-06 20:06:01"
//    println(str.substring(0,10).replaceAll("-", ""))

      //LONG最大值-时间戳
//    println(Long.MaxValue - System.currentTimeMillis())


      //字符串所占字节数测试
//    val s1="123456"
//    val s2="abcdef_"
//    val s3="你好"
//    val s4="【】"
//    println(s1.getBytes.length)
//    println(s2.getBytes.length)
//    println(s3.getBytes.length)
//    println(s4.getBytes.length)
//    println(System.getProperty("file.encoding"))
//
//    def getTomorrow(tm: Long): Long = {
//      val fm = new SimpleDateFormat("yyyy-MM-dd")
//      val tim = fm.format(new Date(tm))
//      val date = fm.parse(tim)
//      val cal = Calendar.getInstance()
//      cal.setTime(date)
//      cal.add(Calendar.DATE, 1)
//      cal.getTimeInMillis
//    }
//
//    println(TimeUtil.timestampToString(getTomorrow(System.currentTimeMillis())))

    //将string类型转为boolean类型
//    def transStrToBool(str:String)={
//      val result: Boolean = str match {
//        case "1" => true
//        case "0" => false
//        case _ => throw new UnsupportedOperationException(s"传入的字符串类型${str}无法转为boolean类型")
//      }
//      result
//    }
//
//    println(transStrToBool("1"))

//    def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss") = {
//      val timeFormat = new SimpleDateFormat(format)
//      val timestamp = timeFormat.parse(timeStr).getTime
//      timestamp
//      //    new Timestamp(timestamp)
//    }
//
//    println("0vegk".toLong)


    //两个数组，判断一个数组是否在另一个数组中

//    def  isContains[T](arr1 :Array[T], arr2:Array[T]): Boolean ={
//
//      val hsets: mutable.Set[T] = mutable.Set[T]()
//      var result:Boolean=true
//
//      for (elem <- arr1) {
//        if(!hsets.contains(elem)){
//          hsets.add(elem)
//        }
//      }
//
//      for (elem2 <- arr2) {
//        if(!hsets.contains(elem2)){
//          result=false
//        }
//      }
//
//      result
//
//    }
    /**
      * 1.第二个数组中存在相同元素，结果返回的是true，
      * 2.第二个数组中如果是两个key如果字段代表的含义不一致，会与第一个数组中的元素出现冲突
      * gameid+policyid+punishisrc  ,gameid+punishisrc
      *
      */
    //    println(isContains[Int](Array(1, 2, 3,6), Array(1, 2)))
//    println(isContains[Int](Array(1, 2, 3,6), Array(2, 2)))
//    println(isContains[Int](Array(1, 2, 3,6), Array(2, 6)))


    //移除list中的元素
//    val list=ListBuffer("aaa","bbb","ccc")
//    list.remove(list.indexOf("bbb"))
//    println(list)

    //求比率
//    println(3 * 1.0 / 6)
    //        var rate:Double=0.0
    //        if (count2!=0){
    //          rate =(count2*1.0)/count1
    //        }

//    val str="aaa|!bbb|ccc&ddd"
//    println(str.contains('|'))
//    val arr: Array[String] = str.split('|')
//    println(arr.mkString(","))

//    var str2 = "(a|b)&g|h|(e|f)"
//    val pattern = new Regex("(?<=\\()[^\\)]+")
//    //取出括号内需要计算的逻辑
//    val regex_arr: Array[String] = pattern.findAllIn(str2).toArray
//    for (elem <- regex_arr) {
//      //把每次括号内计算的结果都保存下来
//      val result: Boolean = true
//      str2=str2.replace("("+elem+")",result.toString)
//    }
//
//    println(str2)

//    println(",".charAt(0))

//    println("aaa&&bbb".contains("&&"))

//    println("aaa||bbb".split("\\|\\|").mkString(","))
//    println("aaa|bbb".contains("|"))

//    val str="id"
//    val arr: Array[String] = str.split(",")
//    val test: ArrayBuffer[Expression] =  ArrayBuffer[Expression]()
//    for (elem <- arr) {
//      test.append(Expression)
//    }
//    println(test.toArray.mkString(","))

//    val result: Expression = ExpressionParser.parseExpression(str)
//    println(result.toString)

    //将数组变为多个参数传入
//    def compute(elem:String*): Unit ={
//      println(elem)
//    }
//    val tt=Array("1","2")
//    compute(tt:_*)


//    val inputStr="data_time\tSTRING\t时间\ngame_id\tBIGINT\t游戏ID\naccount\tSTRING\t账号\nmap_name\tSTRING\t图片名\nmodel_result\tSTRING\t模型输出结果"
//
//    val lines: Array[String] = inputStr.split("\n")
//    val columnNameArr: ArrayBuffer[String] = ArrayBuffer[String]()
//    for (elem <- lines) {
//      val dataArr: Array[String] = elem.split("\t")
//      columnNameArr.append(dataArr.head)
//    }
//    println(columnNameArr.mkString(","))


//    val str="id='a'"
//    println(str.replaceAll("'", ""))

    //两个整数相除，精确到小数点后n位
//    val a=26654
//    val b=2899
//    val newa= BigDecimal(a)
//    val newB= BigDecimal(b)
//   val result= (a*1.0 / b).formatted("%.3f")
//    println(result)




//    val test: mutable.Map[String, String] = mutable.Map[String,String]()
//    test.put("1","a")
//    println(test.mkString(","))

//    println(1E-5.toFloat)


    //数组截取测试
//    val arr = Array("12","13","14","15","false","true","sadf#adf")
//    println(arr.takeRight(3).head)
//    println(arr.dropRight(2).mkString(","))
//    println("rawdata_rotation_deltay_abnoml_cnt".hashCode%26)
//    println("rawdata_rotation_deltay_d_abnoml_cnt".hashCode%26)

//    println(TimeUtil.timestampToString(tomorrowZeroTimestampMs(System.currentTimeMillis(), 8)))

//    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = {
//      println((now + timeZone * 3600000))
//      println((now + timeZone * 3600000)% 86400000)
//      now - (now + timeZone * 3600000) % 86400000+ 86400000
//    }

//    println("2020-11-25 14:04:23".toCharArray.length)
//    println("2020-11-25".toCharArray.length)
//    println("20201125".toCharArray.length)

//      val list = List("1","1","1")
//    println(list)

//    val list = List("1","2","3","4")
//    println(list.drop(3))


//    println("CF穿越火线->封号问题->帐号封号".contains("CF穿越火线->封号问题"))
//    println("2020-12-18 13:07:39".length)

//    val arr = new Array[String](3)
//    println(new Array[String](3).mkString("|"))

//    println((1 + 0.00234).toLong)
//    println(("10").toLong)

    /**
      *
    ／／判断整数（int）
private boolean isInteger(String str) {
	if (null == str || "".equals(str)) {
		return false;
	}
	Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
	return pattern.matcher(str).matches();
}

／／判断浮点数（double和float）
private boolean isDouble(String str) {
	if (null == str || "".equals(str)) {
		return false;
	}
	Pattern pattern = Pattern.compile("^[-\+]?\d*[.]\d+$"); // 之前这里正则表达式错误，现更正
	return pattern.matcher(str).matches();

      */



    //    var str2 = "(a|b)&g|h|(e|f)"
    //    val pattern = new Regex("(?<=\\()[^\\)]+")
    //    //取出括号内需要计算的逻辑
    //    val regex_arr: Array[String] = pattern.findAllIn(str2).toArray
    //    for (elem <- regex_arr) {
    //      //把每次括号内计算的结果都保存下来
    //      val result: Boolean = true
    //      str2=str2.replace("("+elem+")",result.toString)
    //    }

//    def isBigInt(str:String): Boolean ={
//      if (null == str || ""== str) {
//        return false
//      }
//      val  pattern = Pattern.compile("^[-\\+]?[\\d]*$")
//      val bool: Boolean = pattern.matcher(str).matches()
//      bool
//    }
//
//    println(isBigInt("30"))

//    println(Random.nextInt(10))
//    println(Random.nextInt(10))
//    println(Random.nextInt(10))
//    println(Random.nextInt(10))
//    println(1E-
//    val keyCounts="3".toInt
//    val dataCounts="1".toInt*10000
//    val keybyCoutns=dataCounts/keyCounts
//    println(keybyCoutns)
//
//		println((1*1.0 / 1000))
//		println(((100000 / 1000) * 10000).toInt)

		// List[List[String]]







  }

}
