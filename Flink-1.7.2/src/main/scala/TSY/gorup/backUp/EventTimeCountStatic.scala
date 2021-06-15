package TSY.gorup.backUp

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._
import  scala.collection.JavaConverters._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * \*/
object EventTimeCountStatic{
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        override def extractTimestamp(element: String): Long = {
          stringToTimestamp(element.split('|')(0))
        }
      })

    //是否去重
    val is_distinct=if("1"=="1")true else false
    //联合主键位置
    val main_key_pos="2"
    //统计字段位置
    val field_pos="3".toInt-1
    //时间字段位置
    val time_pos="1".toInt-1
    //统计结果时长(秒)
    val time_valid="9999".toLong


    val LOG = LoggerFactory.getLogger(this.getClass)



    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000



    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
      private var timeFieldMapState:MapState[String,String]=_
      private var timeFieldListState:ListState[String]=_
      private var timeState: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        timeFieldMapState=getRuntimeContext.getMapState(
          new MapStateDescriptor[String,String]("timeFieldMapState",classOf[String],classOf[String])
        )
        timeFieldListState=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("timeFieldListState",classOf[String])
        )
        timeState = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
      }

      override def processElement(input: String,
                                  ctx: KeyedProcessFunction[String, String, String]#Context,
                                  out: Collector[String]): Unit = {
        val input_arr: Array[String] = input.split('|')
        val field=input_arr(field_pos)
        val ts=stringToTimestamp(input_arr(time_pos))
        var timer=0L
        var key=""


        //选用时间范围为当天
        if(time_valid==9999L){
          key=ctx.getCurrentKey+"_"+ctx.timerService().currentProcessingTime()
          if(timeState.value()==0L){
            val tomorrowTS=tomorrowZeroTimestampMs(ts,8)
            timer=tomorrowTS
            ctx.timerService().registerEventTimeTimer(tomorrowTS)
            LOG.info(s"""${ctx.getCurrentKey}注册了定时器${tomorrowTS}""")
            timeState.update(tomorrowTS)
          }
        }
        else{
          timer=ts+(time_valid*1000)
          key=ctx.getCurrentKey+"_"+timer
          ctx.timerService().registerEventTimeTimer(timer)
        }

        var counts=0

        //判断是否去重
        if(is_distinct){
          //key_234234,1534280186
          timeFieldMapState.put(key,field)
          import scala.collection.JavaConversions._
          val fieldSet: Set[String] = timeFieldMapState.values().iterator().toSet
          counts = fieldSet.size
        }else {
          timeFieldListState.add(key)
          import scala.collection.JavaConversions._
          val list: List[String] = timeFieldListState.get().iterator().toList
          counts=list.size
        }
        out.collect(input+"|"+counts)
      }

      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        if(time_valid==9999L){
          timeFieldMapState.clear()
          timeFieldListState.clear()
          timeState.clear()
          LOG.info(s"""定时器触发了，本次清除的key_time是${ctx.getCurrentKey+"_"+timestamp}""".stripMargin)
        }
        else{
          val key=ctx.getCurrentKey+"_"+timestamp
          if(is_distinct){
            if(timeFieldMapState.keys()!=null){
              timeFieldMapState.remove(key)
            }
//            println(s"""定时器触发了，本次清除的keytime是${ctx.getCurrentKey+"_"+timestamp}""".stripMargin)
//            println(timeFieldMapState.keys().toList.mkString(">>"))
          }
          else{
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
            }
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