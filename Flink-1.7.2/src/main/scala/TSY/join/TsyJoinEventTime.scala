package TSY.join

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/12/4
  * \* Time: 9:43
  * \*/
//目前先提供指定时间范围，全连接

object TsyJoinEventTime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)

   

    val LOG=LoggerFactory.getLogger(this.getClass)


    val mainKey="1|1"
    //时间字段位置
    val timePos="2|2"
    //统计结果时长(秒)
    val time_valid="10".toLong

    val mainKeyPos1=mainKey.split('|').head.toInt-1
    val mainKeyPos2=mainKey.split('|').last.toInt-1
    val timePos1=timePos.split('|').head.toInt-1
    val timePos2=timePos.split('|').last.toInt-1


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
    
    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000



    val inputDS1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)
//      .assignAscendingTimestamps(str=>{
//        stringToTimestamp(str.split('|')(timePos1))
//      })

      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        override def extractTimestamp(element: String): Long = {
         val value: Long = stringToTimestamp( element.split('|')(timePos1))
          println(value)
          value
        }
      })

    val inputDS2: DataStream[String] = env.socketTextStream("9.134.217.5",9998)
      .assignAscendingTimestamps(str=>{
        stringToTimestamp(str.split('|')(timePos2))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        override def extractTimestamp(element: String): Long = {
          val value: Long = stringToTimestamp( element.split('|')(timePos2))
          println(value)
          value
        }
      })
    
    
    val keyedStream1: KeyedStream[String, String] = inputDS1.keyBy(
      str=>{
        val key: String = str.split('|')(mainKeyPos1)
        println(key)
        key
      }
    )
//    (_.split('|')(mainKeyPos1))

    val keyedStream2: KeyedStream[String, String] = inputDS2.keyBy(
    str=>{
      val key: String = str.split('|')(mainKeyPos1)
      println(key)
      key
    }
    )
//    (_.split('|')(mainKeyPos2))



    class JoinCoProcessFunction  extends CoProcessFunction[String,String,String] {

      private var streamCacheListState1:ListState[String]=_
      private var streamCacheListState2:ListState[String]=_
      private var timeFieldListState1:ListState[String]=_
      private var timeFieldListState2:ListState[String]=_
      private var timeState: ValueState[Long] = _


      override def open(parameters: Configuration): Unit = {
        streamCacheListState1=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("streamCacheListState1",classOf[String])
        )
        streamCacheListState2=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("streamCacheListState2",classOf[String])
        )
        timeFieldListState1=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("timeFieldListState1",classOf[String])
        )
        timeFieldListState2=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("timeFieldListState2",classOf[String])
        )
        timeState = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
      }

      override def processElement1(value: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {

        val inputArr: Array[String] = value.split('|')
        val key = inputArr(mainKey.split('|').head.toInt - 1)
        val time=inputArr(timePos1)
        val ts=stringToTimestamp(time)
        val timeKey=key+"_"+time

        var timer=0L
        if(time_valid==9999L){
          if(timeState.value()==0L){
            val tomorrowTS=tomorrowZeroTimestampMs(ts,8)

            ctx.timerService().registerEventTimeTimer(tomorrowTS)
            LOG.info(s"""${timeKey}注册了定时器${tomorrowTS}""")
            println(s"""${timeKey}注册了定时器${timestampToString(tomorrowTS)}""")
            timeState.update(tomorrowTS)
          }
        }

        else{
          timer=ts+(time_valid*1000)
          ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+(time_valid*1000))
          ctx.timerService().registerEventTimeTimer(timer)
          println(s"""${timeKey}注册了定时器${timestampToString(timer)}""")
        }

        streamCacheListState1.add(value)
        import scala.collection.JavaConverters._
        val streamList2: List[String] = streamCacheListState2.get().iterator().asScala.toList
        if(streamList2.size>0){
          for (elem <- streamList2) {
            out.collect(value+elem)
          }
        }else{
          out.collect(value)
        }


      }

      override def processElement2(value: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        val inputArr: Array[String] = value.split('|')
        val key = inputArr(mainKey.split('|').last.toInt - 1)
        val time=inputArr(timePos2)
        val ts=stringToTimestamp(time)
        val timeKey=key+"_"+time

        var timer=0L
        if(time_valid==9999L){
          if(timeState.value()==0L){
            val tomorrowTS=tomorrowZeroTimestampMs(ts,8)
            ctx.timerService().registerEventTimeTimer(tomorrowTS)
            LOG.info(s"""${timeKey}注册了定时器${tomorrowTS}""")
            timeState.update(tomorrowTS)
          }
        }

        else{
          timer=ts+(time_valid*1000)
          ctx.timerService().registerEventTimeTimer(timer)
        }

        streamCacheListState2.add(value)

        val streamList1: List[String] = streamCacheListState1.get().iterator().asScala.toList
        for (elem <- streamList1) {
          out.collect(value+elem)
        }

      }

      override def onTimer(timestamp: Long, ctx: CoProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
        //定时器触发，需要清理1流和2流的过期数据
        println("定时器触发了")
        val streamList1: List[String] = streamCacheListState1.get().iterator().asScala.toList
        println(streamList1)
//        streamList1.indexOf()
//        streamList1.updated(2,"3")


      }


    }

    keyedStream1.connect(keyedStream2)
      .process(new JoinCoProcessFunction)
        .print()




    env.execute()


  }

}