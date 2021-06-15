package TSY.debug

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.flink.api.common.state._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * \*/
object ProceeingTimeCountStatic{
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    //需要保证同一时间同一uin只有同一变量

    //统计类型
    val static_type="0"
    //是否去重
    val is_distinct=if("0"=="1")true else false
    //联合主键位置
    val main_key_pos="2"
    //统计字段位置
    val field_pos="3".toInt-1
    //时间字段位置
    val time_pos="1".toInt-1
    //统计结果时长(秒)
    val time_valid="20".toLong


    def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
      val timeFormat = new SimpleDateFormat(format)
      val timestamp = timeFormat.parse(timeStr).getTime
      timestamp
    }

    def timestampToString(timestamp: Long, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
      val timeFormat = new SimpleDateFormat(format)
      val timeStr = timeFormat.format(new Date(timestamp))
      timeStr
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
      private var timeState:ValueState[Long]=_
      //把谁注册了什么内容记录下来
      private var timeFieldMapState:MapState[String,String]=_
      private var timeFieldListState:ListState[String]=_

      override def open(parameters: Configuration): Unit = {
        timeState=getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
        timeFieldMapState=getRuntimeContext.getMapState(
          new MapStateDescriptor[String,String]("timeFieldMapState",classOf[String],classOf[String])
        )
        timeFieldListState=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("timeFieldListState",classOf[String])
        )
      }

      override def processElement(input: String,
                                  ctx: KeyedProcessFunction[String, String, String]#Context,
                                  out: Collector[String]): Unit = {
        val input_arr: Array[String] = input.split('|')
        val field=input_arr(field_pos)

        val timer=ctx.timerService().currentProcessingTime()+(time_valid*1000)
        ctx.timerService().registerProcessingTimeTimer(timer)
        println(s"定时器已经注册，注册的定时器为：${(timer)}")
        val key=ctx.getCurrentKey+"_"+timer.toString
        var counts=0

        if(is_distinct){
          //这一步是为了定时器缓存 1534280186_2020-10-30 17:34:37,333,把其中的所有value都拉出来，拉出来放到set中做去重操作
          timeFieldMapState.put(key,field)
          val fieldSet: Set[String] = timeFieldMapState.values().iterator().toSet
          counts = fieldSet.size
          println("当前timeFieldMapState状态中的值是：>>>>>")
          val it=timeFieldMapState.entries().iterator()
          while(it.hasNext){
            val next=it.next()
            println( next.getKey)
            println( next.getValue)
          }
          println("遍历结束》》》》")
        }else {
          timeFieldListState.add(key)
          val list: List[String] = timeFieldListState.get().iterator().toList
          counts=list.size
          println("当前timeFieldList状态中的值是：>>>>>")
          val it=timeFieldListState.get().iterator()
          while(it.hasNext){
            val next=it.next()
            println( s"""${next.split('_').head+"_"+timestampToString(next.split('_').last.toLong)}""")
          }
          println("遍历结束》》》》")
        }




          timeState.update(timer)
          //把比之前时间戳小的状态全部干掉
        //这里的set不再是由状态中获取而来，而是每次来一条数据都会把状态数据放到set集合中去重

        out.collect(input+"|"+counts)


      }

      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        println(">>>>>>>>>>>>>>>>>>>>>定时器触发》》》》》》》》》》》》》》》》》》》》》")
        //注册的定时器到了，这个时候需要做的是更新其中的状态也就是把注册的那条数据删除
        val key=ctx.getCurrentKey+"_"+timestamp
        if(is_distinct){
          if(timeFieldMapState.keys()!=null){
            timeFieldMapState.remove(key)
            println("当前timeFieldMapState状态中的值是：>>>>>")
            val it=timeFieldMapState.entries().iterator()
            while(it.hasNext){
              val next=it.next()
              println( next.getKey)
              println( next.getValue)
            }
            println("遍历结束》》》》")
            out.collect(
              s"""定时器触发了，本次清除的keytime是${ctx.getCurrentKey+"_"+(timestamp)},
                 |本次清除的key${ctx.getCurrentKey},本次清除的timestamp是${(timestamp)}""".stripMargin)
          }
        }else{
           if(timeFieldListState!=null){
             val resultList= new util.LinkedList[String]()
             val it: util.Iterator[String] =timeFieldListState.get().iterator()
             while(it.hasNext){
               val next=it.next()
               if(next!=key){
                 resultList.add(next)
               }
             }
             timeFieldListState.update( resultList)


             println("当前timeFieldList状态中的值是：>>>>>")
             val test: List[String] = timeFieldListState.get().iterator().toList
             for (next <- test) {
               println( s"""${next.split('_').head+"_"+timestampToString(next.split('_').last.toLong)}""")
             }
             println("遍历结束》》》》")

           }
        }

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