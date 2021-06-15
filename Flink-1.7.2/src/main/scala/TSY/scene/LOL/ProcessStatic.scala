package TSY.scene.LOL
import java.util

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api. TimeCharacteristic
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import scala.collection.mutable

object ProcessStatic {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val inputDS: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    val mainKey="2"
    val fieldPos="3".toInt-1
    val fieldSplitChar="+".charAt(0)
    //统计结果时长(秒)
    val time_valid="9999".toLong
    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000
    class StringSumKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {

      private var cacheValueState:ValueState[mutable.Set[String]]=_
      private var timeState:ValueState[Long]=_


      //初始化状态
      override def open(parameters: Configuration): Unit = {
        cacheValueState=getRuntimeContext.getState(
          new ValueStateDescriptor[mutable.Set[String]]("cacheValueState", classOf[mutable.Set[String]])
        )
        timeState=getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
      }

      //处理流中的每个元素
      override def processElement(input: String,
                                  ctx: KeyedProcessFunction[String, String, String]#Context,
                                  out: Collector[String]): Unit = {
        val inputArr: Array[String] = input.split('|')

        val ts=ctx.timerService().currentProcessingTime()
        var timer = 0L
        if (time_valid == 9999L) {
          if (timeState.value() == 0L) {
            val tomorrowTS = tomorrowZeroTimestampMs(ts, 8)
            ctx.timerService().registerProcessingTimeTimer(tomorrowTS)
            timer = tomorrowTS
            timeState.update(tomorrowTS)
          }
        }
        else {
          timer = ts + (time_valid * 1000)
          ctx.timerService().registerProcessingTimeTimer(timer)
        }

        val content: String = inputArr(fieldPos)
        val contentSet: Set[String] = content.split(fieldSplitChar).toSet.filterNot(str=>{
          str==""||str==null
        })
        val cacheSet: mutable.Set[String] = if(cacheValueState.value()==null)mutable.Set[String]()else cacheValueState.value()
        val resSet: mutable.Set[String] = cacheSet++contentSet
        cacheValueState.update(resSet)

        out.collect(input+"|"+resSet.size+"|"+resSet.mkString("+"))

      }

      //第二天定时器到了，需要清空所有缓存状态
      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        cacheValueState.clear()
        timeState.clear()
      }
    }
    val resultDS: DataStream[String] = inputDS
      .keyBy(str=>{
        val arr: Array[String] = str.split('|')
        val mainArr: Array[String] = mainKey.split('|')
        var key = ""
        for (ele <- mainArr) {
          key += arr(ele.toInt - 1)
        }
        key
      })
      .process(new StringSumKeyedProcessFunction)
      .uid("process-string-sum-static")

    resultDS.print()
    env.execute()

  }
}
