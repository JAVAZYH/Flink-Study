package TSY.gorup

import java.util

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * \*/
object ProcessingTimeCountStaticMapState{
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //是否去重
    val is_distinct=if("1"=="1")true else false
    //联合主键位置
    val main_key_pos="2"
    //统计字段位置
    val field_pos="3|4"
    val fieldArr: Array[Int] = field_pos.split('|').map(_.toInt-1)


    //统计结果时长(秒)
    val time_valid="600".toLong


    val LOG = LoggerFactory.getLogger(this.getClass)

    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)


    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
//      private var cacheMapState: ValueState[(mutable.Set[String],mutable.Set[String])] = _
      private var cacheMapState: MapState[Long,(mutable.Set[String],mutable.Set[String])]  = _


      override def open(parameters: Configuration): Unit = {
        cacheMapState=getRuntimeContext.getMapState(
          new MapStateDescriptor[Long,(mutable.Set[String],mutable.Set[String])]
          ("cacheMapState",classOf[Long],classOf[(mutable.Set[String],mutable.Set[String])])
        )
      }

      override def processElement(input: String,
                                  ctx: KeyedProcessFunction[String, String, String]#Context,
                                  out: Collector[String]): Unit = {
        val input_arr: Array[String] = input.split('|')
        val timer = ctx.timerService().currentProcessingTime() + (time_valid * 1000)
        ctx.timerService().registerProcessingTimeTimer(timer)
        
        var fieldSet: mutable.Set[String] = null
        var fieldSet2: mutable.Set[String] = null

        val it: util.Iterator[(mutable.Set[String], mutable.Set[String])] = cacheMapState.values().iterator()
        while (it.hasNext){
          val cacheTuple: (mutable.Set[String], mutable.Set[String]) = it.next()
          fieldSet.addAll(cacheTuple._1)
          fieldSet2.addAll(cacheTuple._2)
        }

        val field1=input_arr(fieldArr.head)
        val field2=input_arr(fieldArr.last)
        var counts1=0
        var counts2=0
        if(null==field1 || field1=="null" || field1==""){
              counts1=fieldSet.size
        }else{
              fieldSet.add(field1)
              counts1=fieldSet.size
            }
        if(null==field2 || field2=="null" || field2==""){
              counts2=fieldSet2.size
        }else{
              fieldSet2.add(field2)
              counts2=fieldSet2.size
        }
        out.collect(input+"|"+counts1+"|"+counts2)

      }

      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        println("定时器触发了")
        if(null!=cacheMapState){
        cacheMapState.remove(timestamp)
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
      .uid("processTimeCount")

    resultDS.print()
    env.execute()
  }


}