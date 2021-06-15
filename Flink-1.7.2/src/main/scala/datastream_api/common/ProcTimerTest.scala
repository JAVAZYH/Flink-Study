package datastream_api.common

import MyUtil.TimeUtil
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/9/29
  * \* Time: 15:37
  * \*/
//基于处理时间和事件时间，注册定时器，查看定时器清空的时机
object ProcTimerTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[String] = env.addSource(new MySource.TeacherSource)
    //统计每个id出现的老师数量
    input.keyBy(_.split('|').head)
      .process(new KeyedProcessFunction[String,String,String] {
        private var namesState:ValueState[String]=_
        private var countState:ValueState[Long]=_
        private var timeState:ValueState[Long]=_

        override def open(parameters: Configuration): Unit = {
          namesState=getRuntimeContext.getState(
            new ValueStateDescriptor[String]("namesState",classOf[String])
          )
          countState=getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("countState",classOf[Long])
          )
          timeState=getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("timeState",classOf[Long])
          )
        }
        override def processElement(value: String,
                                    ctx: KeyedProcessFunction[String, String, String]#Context,
                                    out: Collector[String]): Unit = {
          val input_arr: Array[String] = value.split('|')
          val id=input_arr.head
          val name=input_arr(1)
          val time=input_arr(2)

          if(timeState.value()==0L){
            val ts: Long = ctx.timerService().currentProcessingTime()
            val timer=ts+5*1000
            ctx.timerService().registerProcessingTimeTimer(timer)
            timeState.update(timer)
          }

          namesState.update(if(namesState.value()==null) ""+name else namesState.value()+"#"+name)
          countState.update(countState.value()+1L)

          out.collect(value+"|"+namesState.value()+"|"+countState.value())
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
          namesState.clear()
          countState.clear()
          timeState.clear()
          out.collect("定时器触发了,触发的是"+TimeUtil.timestampToString(timestamp)+
          s"""触发的key是:${ctx.getCurrentKey}""")
        }
      }
      )
        .print()


    env.execute()

  }

}