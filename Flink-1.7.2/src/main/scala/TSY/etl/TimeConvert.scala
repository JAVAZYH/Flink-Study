package TSY.etl

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/22
  * \* Time: 18:16
  * \*/
object TimeConvert {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input_ds: DataStream[String] = env.fromCollection(Seq[String]("2020-10-22 18:18:10|2","2020-10-22 20:18:10|2020-10-22 18:18:10"))


    val timePos="1|2"

    val fromTimePattern="yyyy-MM-dd HH:mm:ss"

    val toTimePattern="time_stamp_second"

    val nullReplace: Boolean ="0" match{
      case "0"=>false
      case "1"=>true
    }



    /**
      *
      * @param timestamp 传入的时间戳，单位毫秒
      * @param format
      * @return 默认返回"yyyy-MM-dd HH:mm:ss" 的时间字符串
      */
    def timestampToString(timestamp: Long, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
      val timeFormat = new SimpleDateFormat(format)
      val timeStr = timeFormat.format(new Date(timestamp))
      timeStr
    }

    /**
      *
      * @param timeStr 传入"yyyy-MM-dd HH:mm:ss"的字符串格式
      * @param format
      * @return 返回时间戳，单位：毫秒
      */
    def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss") = {
      val timeFormat = new SimpleDateFormat(format)
      val timestamp = timeFormat.parse(timeStr).getTime
      timestamp
    }

    def checkTimePattern(input:String,timePattern: String): Boolean ={
     val inputLength=input.length
     val result: Boolean = timePattern match {
       case "time_stamp_second" => inputLength == 10
       case "time_stamp_mil_second" => inputLength == 13
       case "yyyy-MM-dd HH:mm:ss" => inputLength == 19
       case "yyyy/MM/dd HH:mm:ss" =>inputLength == 19
       case "yyyyMMddHHmmss" =>inputLength == 14
       case "yyyy-MM-dd" =>inputLength == 10
       case "yyyyMMdd" =>inputLength == 8
       case "yyyy/MM/dd" =>inputLength == 10
       case _=>false
     }
     result
   }



    def TimePatternConvert(time:String,fromTimePattern: String,toTimePattern: String) ={
      var resTime=""
      if(fromTimePattern=="time_stamp_mil_second"){
        if(checkTimePattern(time,fromTimePattern)) resTime=timestampToString(time.toLong,toTimePattern) else {
          if(nullReplace)resTime=null else resTime=time
        }
      }
      else if(fromTimePattern=="time_stamp_second"){
        if(checkTimePattern(time,fromTimePattern)) resTime=timestampToString(time.toLong*1000,toTimePattern) else  {
          if(nullReplace)resTime=null else resTime=time
        }
      }
      else if(toTimePattern=="time_stamp_mil_second"){
        if(checkTimePattern(time,fromTimePattern)) resTime=stringToTimestamp(time,fromTimePattern).toString else  {
          if(nullReplace)resTime=null else resTime=time
        }
      }
      else if(toTimePattern=="time_stamp_second"){
        if(checkTimePattern(time,fromTimePattern)) resTime=stringToTimestamp(time,fromTimePattern).toString.substring(0,10) else  {
          if(nullReplace)resTime=null else resTime=time
        }
      }
      else{
        if(checkTimePattern(time,fromTimePattern)){
        val tmpTime: Long = stringToTimestamp(time,fromTimePattern)
        resTime=timestampToString(tmpTime,toTimePattern)
        }
        else {
          if(nullReplace)resTime=null else resTime=time
        }
      }
      resTime
    }


    def TimeConvert(input_ds:  DataStream[String],timePos: String,fromTimePattern: String,toTimePattern: String) ={
      input_ds.map(str=>{
        val input_arr: Array[String] = str.split('|')
        val timePosArr: Array[Int] = timePos.split('|').map(_.toInt-1)
        for (timePos <- timePosArr) {
          val inputTime:String=input_arr(timePos)
          val resultTime: String = TimePatternConvert(inputTime,fromTimePattern,toTimePattern)
          input_arr.update(timePos,resultTime)
        }
        input_arr.mkString("|")
      })
    }

    val res: DataStream[String] = TimeConvert(input_ds,timePos,fromTimePattern,toTimePattern)
    res.print()
    env.execute()


  }

}