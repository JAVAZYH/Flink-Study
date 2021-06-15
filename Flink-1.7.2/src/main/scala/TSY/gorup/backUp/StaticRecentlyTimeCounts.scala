//package TSY.gorup
//
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//// AllBlue编译时会自动替换package
//import java.text.SimpleDateFormat
//import java.util
//import java.util.Date
//import org.apache.flink.api.common.state._
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.scala.DataStream
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.types.Row
//import org.apache.flink.util.Collector
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//import org.slf4j.LoggerFactory
//import scala.collection.JavaConversions._
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: aresyhzhang
//  * \* Date: 2021/1/4
//  * \* Time: 16:11
//  * \*/
//object StaticRecentlyTimeCounts {
//  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//
//
//    //统计类型
//    // val static_type="0"
//    //是否去重
//    val is_distinct=true
//    //联合主键位置
//    val main_key_pos="1"
//    //统计字段位置
//    val field_pos="3".toInt-1
//    val timeValidPos="23".toInt-1
//
//
//
//    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
//      private var timeFieldMapState:MapState[String,String]=_
//      private var timeFieldListState:ListState[String]=_
//
//      override def open(parameters: Configuration): Unit = {
//        timeFieldMapState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[String,String]("timeFieldMapState",classOf[String],classOf[String])
//        )
//        timeFieldListState=getRuntimeContext.getListState(
//          new ListStateDescriptor[String]("timeFieldListState",classOf[String])
//        )
//      }
//
//      override def processElement(input: String,
//                                  ctx: KeyedProcessFunction[String, String, String]#Context,
//                                  out: Collector[String]): Unit = {
//        val input_arr: Array[String] = input.split('|')
//        val field=input_arr(field_pos)
//
//        val time_valid=input_arr(timeValidPos).toLong
//        val timer=ctx.timerService().currentProcessingTime()+(time_valid*1000)
//        ctx.timerService().registerProcessingTimeTimer(timer)
//        val key=ctx.getCurrentKey+"_"+timer.toString
//        var counts=0
//        if(is_distinct){
//          //这一步是为了定时器缓存 1534280186_2020-10-30 17:34:37,333,把其中的所有value都拉出来，拉出来放到set中做去重操作
//          timeFieldMapState.put(key,field)
//          val fieldSet: Set[String] = timeFieldMapState.values().iterator().toSet
//          counts = fieldSet.size
//        }else {
//          timeFieldListState.add(key)
//          val list: List[String] = timeFieldListState.get().iterator().toList
//          counts=list.size
//        }
//        out.collect(input+"|"+counts)
//      }
//
//      override def onTimer(timestamp: Long,
//                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
//                           out: Collector[String]): Unit = {
//
//
//        //注册的定时器到了，这个时候需要做的是更新其中的状态也就是把注册的那条数据删除
//        val key=ctx.getCurrentKey+"_"+timestamp
//        if(is_distinct){
//          if(timeFieldMapState.keys()!=null){
//            timeFieldMapState.remove(key)
//          }
//
//        }
//        else{
//          if(timeFieldListState!=null){
//            val resultList= new util.LinkedList[String]()
//            val it: util.Iterator[String] =timeFieldListState.get().iterator()
//            while(it.hasNext){
//              val next=it.next()
//              if(next!=key){
//                resultList.add(next)
//              }
//            }
//            timeFieldListState.update( resultList)
//          }
//
//        }
//
//
//      }
//    }
//
//    val resultDS: DataStream[String] = input_DStream1
//      .keyBy(str => {
//        val arr: Array[String] = str.split('|')
//        val mainArr: Array[String] = main_key_pos.split('|')
//        var key = ""
//        for (ele <- mainArr) {
//          key += arr(ele.toInt - 1)
//        }
//        key
//      })
//      .process(new CountStaticKeyedProcessFunction)
//      .uid(context.genOperatorUid("1"))
//
//  }
//
//}