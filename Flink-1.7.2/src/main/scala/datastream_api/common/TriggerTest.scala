package datastream_api.common

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * 自定义触发器，实现100条数据触发一次计算，5分钟内数据不满100条也触发窗口的计算
  */
object TriggerTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[String] = env.addSource(new MySource.TeacherSource)

    class MyTrigger extends Trigger {
      override def onElement(element: Nothing, timestamp: Long, window: Nothing, ctx: Trigger.TriggerContext): TriggerResult = ???

      override def onProcessingTime(time: Long, window: Nothing, ctx: Trigger.TriggerContext): TriggerResult = ???

      override def onEventTime(time: Long, window: Nothing, ctx: Trigger.TriggerContext): TriggerResult = ???

      override def clear(window: Nothing, ctx: Trigger.TriggerContext): Unit = ???
    }


    input.keyBy(_.split("|")(1))
      .timeWindow(Time.seconds(10))
      .trigger( CountTrigger.of(2))

    input.print()
    env.execute()

  }



}
