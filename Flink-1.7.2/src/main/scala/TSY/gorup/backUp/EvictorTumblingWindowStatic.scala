package TSY.gorup.backUp

import java.text.SimpleDateFormat

import MyUtil.TimeUtil
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.flink.api.scala._
/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * 使用滚动窗口统计指定时间范围内的量级统计（可选是否去重）
  * \*/
object EvictorTumblingWindowStatic {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


//    //联合主键位置
//    val main_key_pos="2"
//    //统计字段位置
//    val field_pos="3".toInt-1
//    //时间字段位置
//    val time_pos="1".toInt-1
//    val time_size="86400".toLong
//    val slide_size="3600".toLong

    //统计类型,0表示量级，1表示内容，2表示两者都选
    val static_type="0"
    //是否去重
    val is_distinct="1"
    //联合主键位置
    val main_key_pos="2"
    //统计字段位置
    val field_pos="3".toInt-1
    //统计结果时长(秒)
    val time_valid="10".toLong

    //时间字段位置
    val time_pos="1".toInt-1

    def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
      val timeFormat = new SimpleDateFormat(format)
      val timestamp = timeFormat.parse(timeStr).getTime
      timestamp
    }

    val input_DStream1: DataStream[String] =
      env.socketTextStream("9.134.217.5",9999)
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
          // TODO 提取每条数据的事件时间(毫秒)
          override def extractTimestamp(element: String): Long = {
            val timeStr: String = element.split('|')(time_pos)
            TimeUtil.stringToTimestamp(timeStr)
          }
        }
      )


    class CountStaticKeyedProcessFunction extends ProcessWindowFunction[String,String,String,TimeWindow] {
      private var countSetState:ValueState[mutable.Set[String]]=_
      private var countLongState:ValueState[Long]=_
      private var contentState:ValueState[mutable.ArrayBuffer[String]]=_
      private var windowStartState:ValueState[Long]=_

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
        windowStartState=getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("windowStartState", classOf[Long])
        )
      }

      override def process(key: String, context: Context, it: Iterable[String], out: Collector[String]): Unit ={
        var count=0L
        var content=""
        val inputList: List[String] = it.toList

        println("窗口开始时间"+TimeUtil.timestampToString(context.window.getStart))
        println("窗口结束时间"+TimeUtil.timestampToString(context.window.getEnd))

        val currentWindowStart=context.window.getStart

        var lastWindowStartTime: Long = windowStartState.value()
        if(lastWindowStartTime==0){
          windowStartState.update(currentWindowStart)
          lastWindowStartTime=currentWindowStart
        }

        //如果说窗口的开始时间大于现在的窗口开始时间，表示已经到下一个窗口已经到了要把上一个窗口的状态清除，但是为了防止有延迟数据还会用到上一个窗口的状态
        //所以在清除之前会把上一个窗口的状态暂时都保留下来，保留的时间就是延迟时间
        if(currentWindowStart!=lastWindowStartTime){
          println(s"当前key:$key  当前的窗口开始时间:${TimeUtil.timestampToString(currentWindowStart)} " +
            s" 之前的窗口开始时间: ${TimeUtil.timestampToString(lastWindowStartTime)} 状态已经过期，clear状态重新计算")
          //所以在清除之前会把上一个窗口的状态暂时都保留下来，保留的时间就是延迟时间
          countSetState.clear()
          contentState.clear()
          countLongState.clear()
          windowStartState.update(currentWindowStart)
        }

        //更新去重的量级/内容状态
        def updateCountSetState(inputList: List[String]) ={
          val fieldSet: mutable.Set[String] = if(countSetState.value()==null)mutable.Set[String]()else countSetState.value()
          inputList.foreach(str=>{
            val input_arr: Array[String] = str.split('|')
            val field=input_arr(field_pos)
            fieldSet.add(field)
          })
          countSetState.update(fieldSet)
          fieldSet
        }

        //更新不去重的量级状态
        def updateCountLongState(inputList: List[String]) ={
          var tmp: Long = if(null==countLongState.value) 0 else countLongState.value()
          inputList.foreach(str=>{
            tmp+=1
          })
          countLongState.update(tmp)
          tmp
        }

        //更新不去重的内容状态
        def updateContentState(inputList: List[String]) ={
          val arr: ArrayBuffer[String] = if(contentState.value()==null) ArrayBuffer[String]()else contentState.value()
          inputList.foreach(str=>{
            val input_arr: Array[String] = str.split('|')
            val field=input_arr(field_pos)
            arr.append(field)
          })
          contentState.update(arr)
          arr
        }

        //统计类型
        static_type match{
          //量级统计
          case "0"=>{
            is_distinct match {
              case "1"=>{
                count = updateCountSetState(inputList).size.toLong
              }
              case "0"=>{
                count=updateCountLongState(inputList)
              }
            }
            inputList.foreach(input=>{
              out.collect(input+"|"+count)
            })
          }
          //内容统计
          case "1"=>{
            is_distinct match {
              case "1"=>{//进行去重统计
                content=updateCountSetState(inputList).mkString(",")
              }
              case "0"=>{//不做去重统计
                content=updateContentState(inputList).mkString(",")
              }
            }
            inputList.foreach(input=>{
              out.collect(input+"|"+content)
            })
          }
          //同时统计,这里的同时统计针对的还是单个字段
          case "2"=>{
            is_distinct match {
              case "1"=>{
                content=updateCountSetState(inputList).mkString(",")
                count = updateCountSetState(inputList).size.toLong
              }
              case "0"=>{
                content=updateContentState(inputList).mkString(",")
                count=updateCountLongState(inputList)
              }
            }
            inputList.foreach(input=>{
              out.collect(input+"|"+count+"|"+content)
            })
          }
        }
      }
    }


    val ouputTag=new OutputTag[String]("side")

    val resultDS: DataStream[String] = input_DStream1
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
      .trigger(CountTrigger.of(1))
      .evictor(CountEvictor.of(0,true))
      .sideOutputLateData(ouputTag)
      .process(new CountStaticKeyedProcessFunction)

    resultDS.print()

    resultDS.getSideOutput(ouputTag).print("output>>>>>")

    env.execute()
  }


}