package TSY.gorup

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2021/1/20
  * \* Time: 11:59
  * TSY流量控制
  * 采用keyby+process方式缓存单单挑数据
  * 以时间+key（分钟整点时间_列名_列值）作为key，每分钟输出一条,减少数据量。
  * \*/
object ProcessWindowGroupStatic {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDataStream: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    val main_key_pos="1|2|3"
    val timeValid="10".toLong

    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000

    class WindowGroupProcessFunction extends KeyedProcessFunction[String,String,String]{
      private var cacheValue:ValueState[String]=_
      private var timeState: ValueState[Long] = _
      override def open(parameters: Configuration): Unit = {
        cacheValue=getRuntimeContext.getState(new ValueStateDescriptor[String](
          "cacheValue",classOf[String]
        ))
        timeState = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
      }
      override def processElement(input: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        //注册定时器
        if (timeState.value() == 0L){
          var timer=0L
          val ts=ctx.timerService().currentProcessingTime()
          if (timeValid == 9999L) {
            val tomorrowTS = tomorrowZeroTimestampMs(ts, 8)
            ctx.timerService().registerProcessingTimeTimer(tomorrowTS)
            timer = tomorrowTS
          }
          else {
            timer = ts + (timeValid * 1000)
            ctx.timerService().registerProcessingTimeTimer(timer)
          }
          timeState.update(timer)
        }
        cacheValue.update(input)
      }
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect(cacheValue.value())
        timeState.clear()
        cacheValue.clear()
      }
    }

    val resultDS: DataStream[String] = inputDataStream.keyBy(str => {
      val arr: Array[String] = str.split('|')
      val mainArr: Array[String] = main_key_pos.split('|')
      var key = ""
      for (ele <- mainArr) {
        key += '_'+arr(ele.toInt - 1)
      }
      key
    })
      .process(new WindowGroupProcessFunction)


    resultDS.print()

    env.execute()





  }

}