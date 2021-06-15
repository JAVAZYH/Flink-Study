//package DataStreamAPI
//
//
//import org.apache.flink.api.common.functions.AggregateFunction
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
//
//
///**
//  * * @param <IN>  输入的数据类型
//  * * @param <ACC> 中间结果累加器的数据类型
//  * * @param <OUT> 输出的数据类型
//  */
//class MyAgg extends AggregateFunction[String,Long,Long]{
//  override def createAccumulator(): Long = 0L
//
//  override def add(value: String, accumulator: Long): Long = {
//    accumulator+1L
//  }
//
//  override def getResult(accumulator: Long): Long = accumulator
//
//  override def merge(a: Long, b: Long): Long = a+b
//}
//
//
//class MyWindow extends WindowFunction[Long,String,String,TimeWindow] {
//  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[String]): Unit = {
//    out.collect(key+">>>"+input.iterator.next())
//  }
//}
//
//object CountStatic {
//  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val input_Dstream1 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\lol_report")
//
//    val mainkeyPost=0
//    val timePos=1
//
//
//    val result: DataStream[String] = input_Dstream1
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
//        override def extractTimestamp(str: String): Long = {
//          val timeStr: String = str.split(',')(timePos-1)
//          MyUtil.TimeUtil.stringToTimestamp(timeStr)
//        }
//      })
//      .keyBy(str => {
//        val arr: Array[String] = str.split('|')
//        arr(mainkeyPost)
//      })
//      .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(8)))
//      .trigger(CountTrigger.of(1))
//      .aggregate(new MyAgg, new MyWindow)
//    result.print()
//
//    env.execute()
//
//
//
//  }
//
//}
