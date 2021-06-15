package TSY.gorup.backUp

import java.text.SimpleDateFormat

import MyUtil.TimeUtil
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * \*/
object CountCleanAllStatic {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //统计类型
    val static_type="0"
    //是否去重
    val is_distinct="true"
    //联合主键位置
    val main_key_pos="2"
    //统计字段位置
    val field_pos="3".toInt-1
    //时间字段位置
    val time_pos="1".toInt-1
    //统计结果时长(秒)
    val time_valid="5".toLong
    //时间字段类型
    val time_type="yyyy-MM-dd HH:mm:ss"



    def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
      val timeFormat = new SimpleDateFormat(format)
      val timestamp = timeFormat.parse(timeStr).getTime
      timestamp
    }
    //如果过期时间选的是今天
    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000

    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        override def extractTimestamp(element: String): Long = {
          val time: String = element.split('|')(time_pos)
          stringToTimestamp(time)
        }
      })


    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
      private var countSetState:ValueState[mutable.Set[String]]=_
      private var countLongState:ValueState[Long]=_
      private var contentState:ValueState[mutable.ArrayBuffer[String]]=_
      private var timeState:ValueState[Long]=_

      override def open(parameters: Configuration): Unit = {
        countSetState=getRuntimeContext.getState(
          new ValueStateDescriptor[mutable.Set[String]]("countSetState", classOf[mutable.Set[String]])
        )
        countLongState=getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("countLongState", classOf[Long])
        )
        contentState=getRuntimeContext.getState(
          new ValueStateDescriptor[mutable.ArrayBuffer[String]]("contentState", classOf[mutable.ArrayBuffer[String]])
        )
        timeState=getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
      }

      override def processElement(input: String,
                                  ctx: KeyedProcessFunction[String, String, String]#Context,
                                  out: Collector[String]): Unit = {
        val input_arr: Array[String] = input.split('|')
        val field=input_arr(field_pos)
        val time=input_arr(time_pos)

        val ts: Long = time_type match {
          case "timeStamp" => time.toLong
          case "yyyy-MM-dd HH:mm:ss" => stringToTimestamp(time)
        }
        //如果选用当天时间
        if (time_valid==9999L){
          if(timeState.value()==0L) {
            val tomorrow_timer: Long = tomorrowZeroTimestampMs(ts,8)
            ctx.timerService().registerEventTimeTimer(tomorrow_timer)
            timeState.update(tomorrow_timer)
          }
        }
        else{
          if(timeState.value()==0L) {
            val timer = ts + (time_valid * 1000)
            ctx.timerService().registerEventTimeTimer(timer)
            timeState.update(timer)
            println(s"定时器注册了，注册的定时器是${TimeUtil.timestampToString(timer)}")
          }
        }

        var count=0L
        var content=""

        //更新去重的量级/内容状态
        def updateCountSetState() ={
          val fieldSet: mutable.Set[String] = if(countSetState.value()==null)mutable.Set[String]()else countSetState.value()
          fieldSet.add(field)
          countSetState.update(fieldSet)
          fieldSet
        }

        //更新不去重的量级状态
        def updateCountLongState() ={
          var tmp: Long = if(countLongState.value()==null) 0 else countLongState.value()
          tmp+=1
          countLongState.update(tmp)
          tmp
        }

        //更新不去重的内容状态
        def updateContentState() ={
          val arr: ArrayBuffer[String] = if(contentState.value()==null) ArrayBuffer[String]()else contentState.value()
          arr.append(field)
          contentState.update(arr)
          arr
        }

        //统计类型
        static_type match{
          //量级统计
          case "0"=>{
            is_distinct match {
              case "true"=>{
                count = updateCountSetState().size.toLong
              }
              case "false"=>{
                count=updateCountLongState()
              }
            }
            out.collect(input+"|"+count)
          }
          //内容统计
          case "1"=>{
            is_distinct match {
              case "true"=>{//进行去重统计
                content=updateCountSetState().mkString(",")
              }
              case "false"=>{//不做去重统计
                content=updateContentState().mkString(",")
              }
            }
            out.collect(input+"|"+content)
          }
          //同时统计
          case "2"=>{
            is_distinct match {
              case "true"=>{
                content=updateCountSetState().mkString(",")
                count = updateCountSetState().size.toLong
              }
              case "false"=>{
                content=updateContentState().mkString(",")
                count=updateCountLongState()
              }
            }
            out.collect(input+"|"+count+"|"+content)
          }
        }
      }

      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        println(s"定时器触发了，触发的时间是${TimeUtil.timestampToString(timestamp)},触发的key是${ctx.getCurrentKey}")
                countLongState.clear()
                countSetState.clear()
                contentState.clear()
                timeState.clear()
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
      .process(new CountStaticKeyedProcessFunction)

    resultDS.print()
    env.execute()
  }


}