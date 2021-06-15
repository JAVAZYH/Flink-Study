//package TSY.scene.dzlx
//
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.util.Collector
//import org.slf4j.LoggerFactory
//
//import scala.collection.mutable.ArrayBuffer
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: aresyhzhang
//  * \* Date: 2020/12/29
//  * \* Time: 19:04
//  * 统计每个策略号延迟一段时间后的策略量级
//  *
//  * \*/
//object delayPunishStatic {
//  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    env.setParallelism(1)
//
//    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)
//    val mainKeyPos="4|5".split('|').map(_.toInt-1)
//    val delayTimePos="3".toInt-1
//
//    class delayPunishKeyedProcessFunction    extends KeyedProcessFunction[String,String,String] {
//      private var countMapState: MapState[String,Long]=_
//      private var sumMapState: MapState[String,Double]=_
//      private var timeState: ValueState[Long] = _
//
//      override def open(parameters: Configuration): Unit = {
//
//        countMapState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[String,Long]("cacheMapState",classOf[String],classOf[Long])
//        )
//        sumMapState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[String,Double]("sumMapState",classOf[String],classOf[Double])
//        )
//        timeState = getRuntimeContext.getState(
//          new ValueStateDescriptor[Long]("timeState", classOf[Long])
//        )
//
//      }
//
//      override def processElement(input: String,
//                                  ctx: KeyedProcessFunction[String, String, String]#Context,
//                                  out: Collector[String]): Unit = {
//        val input_arr: Array[String] = input.split('|')
//        val delayTime=input_arr(delayTimePos).toInt
//
//        val ts=ctx.timerService().currentProcessingTime()
//        var timer = 0L
//        if (time_valid == 9999L) {
//          if (timeState.value() == 0L) {
//            val tomorrowTS = tomorrowZeroTimestampMs(ts, 8)
//            ctx.timerService().registerProcessingTimeTimer(tomorrowTS)
//            timer = tomorrowTS
//            timeState.update(tomorrowTS)
//          }
//        }
//        else {
//          timer = ts + (time_valid * 1000)
//          ctx.timerService().registerProcessingTimeTimer(timer)
//        }
//        val resultArr: ArrayBuffer[String] = ArrayBuffer[String]()
//        try {
//          if(countFieldArr.head!=(-1)){
//            for (countIndex <- countFieldArr) {
//              val key=ctx.getCurrentKey+"#"+countIndex
//              val count: Long = countMapState.get(key)
//              countMapState.put(key,count+1)
//              resultArr.append((count+1).toString)
//            }
//          }
//
//          if(sumFieldArr.head!=(-1)){
//            for ( sumIndex <- sumFieldArr) {
//              val key=ctx.getCurrentKey+"#"+sumIndex
//              var cacheSumValue = sumMapState.get(key)
//              val inputSumValue = input_arr(sumIndex).toDouble
//              cacheSumValue+=inputSumValue
//              sumMapState.put(key,cacheSumValue)
//              if(isBigInt( input_arr(sumIndex))){
//                resultArr.append(cacheSumValue.toInt.toString)
//              }else{
//                resultArr.append(cacheSumValue.toString)
//              }
//            }
//          }
//
//          out.collect(input+"|"+resultArr.mkString("|"))
//
//        }catch {
//          case exception: Exception=>{
//            LOG.error(input+"出错了"+exception.getMessage)
//            throw exception
//          }
//        }
//      }
//
//      override def onTimer(timestamp: Long,
//                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
//                           out: Collector[String]): Unit = {
//        LOG.info(s"""定时触发了，触发的key是${ctx.getCurrentKey},触发的时间是${timestamp}""")
//        timeState.clear()
//        countMapState.clear()
//        sumMapState.clear()
//      }
//    }
//
//    val resultDS: DataStream[String] = input_DStream1
//      .keyBy(str => {
//        val arr: Array[String] = str.split('|')
//        var key = ""
//        for (ele <- mainKeyPos) {
//          key += arr(ele.toInt)
//        }
//        key
//      })
//      .process(new CountStaticKeyedProcessFunction)
//      .uid("processing_bloom_count_static")
//
//
//
//
//  }
//
//}