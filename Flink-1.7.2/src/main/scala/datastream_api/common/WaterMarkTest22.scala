//package DataStreamAPI
//
//import java.util.Random
//
//import MyUtil.TimeUtil
//import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.util.Collector
//
//class SensorSource  extends SourceFunction[String]{
//  var isRunning: Boolean = true
//  val ids: List[String] = List("ws111", "ws222", "ws333", "ws444")
//  val vcs: List[String] = List("111", "222", "333", "444")
//
//  private val random = new Random()
//  var number: Long = 0
//
//  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
//    while (isRunning) {
//      ctx.collect(
//        ids(random.nextInt(4))+"|"
//          +TimeUtil.timestampToString(System.currentTimeMillis()) +"|"
//        +vcs(random.nextInt(4)))
//      number += 1
//      Thread.sleep(500)
//      if(number==100){
//        cancel()
//      }
//    }
//
//  }
//  override def cancel(): Unit = {
//    isRunning = false
//  }
//
//}
//
//object WaterMarkTest {
//
//   val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//   val input_DS: DataStream[String] = env.addSource(new SensorSource)
//   env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//   input_DS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(5)) {
//    override def extractTimestamp(element: String): Long = {
//      val arr: Array[String] = element.split('|')
//      TimeUtil.stringToTimestamp(arr(2-1))
//    }
//    })
//    .keyBy(_.split('|')(1-1))
//    .timeWindow(Time.seconds(10),Time.seconds(5))
//
//
//  //    .process(new KeyedProcessFunction[] {})
////
//}
