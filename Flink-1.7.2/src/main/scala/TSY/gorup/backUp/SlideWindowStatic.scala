package TSY.gorup.backUp

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._
import org.apache.flink.api.scala._
import scala.collection.mutable

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * \*/
object SlideWindowStatic {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //联合主键位置
    val main_key_pos="2"
    //统计字段位置
    val field_pos="16".toInt-1
    //时间字段位置
    val time_pos="1".toInt-1
    val time_size="86400".toLong
    val slide_size="3600".toLong


    def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
      val timeFormat = new SimpleDateFormat(format)
      val timestamp = timeFormat.parse(timeStr).getTime
      timestamp
    }
    //如果过期时间选的是今天
    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000

    //可以进行内容统计，或者字段统计，不会在这里提供同时统计的功能，原因是耦合度太高了而且没有必要
    val input_DStream1: DataStream[String] = env.readTextFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\实时\\cf举报\\cf_report.txt")

      .assignAscendingTimestamps(str=>stringToTimestamp(str.split('|')(time_pos)))


    class CountStaticKeyedProcessFunction extends ProcessWindowFunction[String,String,String,TimeWindow] {
      override def process(key: String, context: Context, it: Iterable[String], out: Collector[String]): Unit ={
        val tmpSet: mutable.Set[String] = mutable.Set[String]()
        it.foreach(elem=>{
          val input_arr: Array[String] = elem.split('|')
          val field=input_arr(field_pos)
          tmpSet.add(field)
        })
        out.collect(it.last+"|"+tmpSet.size)
      }
    }


    val resultDS: DataStream[String] = input_DStream1
      .keyBy(str => {
        val arr: Array[String] = str.split('|')
        val mainArr: Array[String] = main_key_pos.split('|')
        var key = ""
        for (ele <- mainArr) {
          key += arr(ele.toInt - 1)
        }
        key
      })
      .timeWindow(Time.seconds(time_size),Time.seconds(slide_size))
      .trigger(CountTrigger.of(1))
      .process(new CountStaticKeyedProcessFunction)

    resultDS.writeAsText("C:\\Users\\aresyhzhang\\Desktop\\临时\\实时\\cf举报\\out")
    env.execute()
  }


}