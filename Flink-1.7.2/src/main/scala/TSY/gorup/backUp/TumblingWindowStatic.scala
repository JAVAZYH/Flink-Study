package TSY.gorup.backUp

import java.text.SimpleDateFormat
import java.util.Date

import MyUtil.TimeUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import org.apache.flink.api.scala._
import scala.collection.mutable

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * 使用滚动窗口统计指定时间范围内的量级统计（可选是否去重）
  * \*/
object TumblingWindowStatic {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    //      .trigger(new Trigger[String,TimeWindow] {
    //        var triggerTimer=0L
    //        override def onElement(element: String, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //           if(triggerTimer==0L){
    //             val timer=ctx.getCurrentProcessingTime+(5)*1000
    //             triggerTimer=timer
    //             println("注册了一个处理时间定时器触发窗口计算"+TimeUtil.timestampToString(timer))
    //             ctx.registerProcessingTimeTimer(timer)
    //           }
    //            TriggerResult.CONTINUE
    //        }
    //        override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //          println("定时器触发了")
    //          triggerTimer=0L
    //          TriggerResult.FIRE
    //        }
    //        override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //          TriggerResult.CONTINUE
    //        }
    //        //当删除窗口时要做的事情
    //        override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //          TriggerResult.FIRE
    ////          ctx.deleteProcessingTimeTimer(triggerTimer)
    //          triggerTimer=0L
    //          println(s" 开始删除窗口，当前的窗口开始时间:${TimeUtil.timestampToString(window.getStart)} " +
    //            s" 当前的窗口结束时间: ${TimeUtil.timestampToString(window.getEnd)}")
    //        }
    //      })


//    //联合主键位置
//    val main_key_pos="2"
//    //统计字段位置
//    val field_pos="3".toInt-1
//    //时间字段位置
//    val time_pos="1".toInt-1
//    val time_size="86400".toLong
//    val slide_size="3600".toLong
    //由于使用了trigger，watermark将会变得没有意义，只有allowlateness会有意义


    //是否去重
    val is_distinct="1"
    //联合主键位置
    val main_key_pos="2"
    //统计字段位置
    val field_pos="3".toInt-1
    //数据延迟时长
    val delay_time="30".toLong
    //统计结果时长(秒)
    val time_valid="86400".toLong

    //时间字段位置
    val time_pos="1".toInt-1

    val LOG = LoggerFactory.getLogger(this.getClass)
    val input_DStream1: DataStream[String] =
      env.socketTextStream("9.134.217.5",9999)
//      .assignTimestampsAndWatermarks(
//        new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(delay_time)) {
//          // TODO 提取每条数据的事件时间(毫秒)
//          override def extractTimestamp(element: String): Long = {
//            val timeStr: String = element.split('|')(time_pos)
//            TimeUtil.stringToTimestamp(timeStr)
//          }
//        }
//      )

    class CountStaticWindowProcessFunction extends ProcessWindowFunction[String,String,String,TimeWindow] {
      override def process(key: String, context: Context, it: Iterable[String], out: Collector[String]): Unit ={
        println(s"当前key:$key  当前的窗口开始时间:${TimeUtil.timestampToString(context.window.getStart)} " +
          s" 当前的窗口结束时间: ${TimeUtil.timestampToString(context.window.getEnd)}")
        is_distinct match{
          case "1"=>{
            val tmpSet: mutable.Set[String] = mutable.Set[String]()
            it.foreach(elem=>{
              val input_arr: Array[String] = elem.split('|')
              val field=input_arr(field_pos)
              tmpSet.add(field)
            })
            out.collect(dateAdd(it.last,time_pos,-8)+"|"+tmpSet.size)
          }
          case "0"=>{
            var counts=0L
            it.foreach(elem=>{
              counts+=1
            })
            out.collect(dateAdd(it.last,time_pos,-8)+"|"+counts)
          }
        }

      }
    }



    def dateAdd(input:String,timePos:Int,hours:Int,format: String = "yyyy-MM-dd HH:mm:ss") ={
      val input_arr=input.split('|')
      val timeStr: String =input_arr (time_pos)
      val timeFormat = new SimpleDateFormat(format)
      val timestamp = timeFormat.parse(timeStr).getTime
      val timeAddStr: String = timeFormat.format(new Date(timestamp+(hours*60*60*1000)))
      input_arr.update(input_arr.indexOf(timeStr),timeAddStr)
      println("addTime>>>>>"+ input_arr.mkString("|"))
      input_arr.mkString("|")
    }


    val ouputTag=new OutputTag[String]("side")

    val resultDS: DataStream[String] = input_DStream1
        .map(input=>{
      dateAdd(input,time_pos,8)
    })
      //这里做了keyby
      .keyBy(str => {
        val arr: Array[String] = str.split('|')
        val mainArr: Array[String] = main_key_pos.split('|')
        var key = ""
        for (ele <- mainArr) {
          key += arr(ele.toInt - 1)
        }
        key
      })
      .timeWindow(Time.seconds(time_valid))
//      .timeWindow(Time.days(1))
      .trigger(CountTrigger.of(1))
      .allowedLateness(Time.seconds(delay_time))
      .sideOutputLateData(ouputTag)
      .process(new CountStaticWindowProcessFunction)


    resultDS.getSideOutput(ouputTag)
        .map(str=>{
          println("output>>>>"+str)
          str
        })



    resultDS.print()
    env.execute()
  }


}