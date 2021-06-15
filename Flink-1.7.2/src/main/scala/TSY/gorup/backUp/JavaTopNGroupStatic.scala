package TSY.gorup.backUp

import java.util

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2021/2/1
  * \* Time: 14:31
  * 对TopN进行统计，指定过期时间，TopN的字段和升序降序规则不固定。
  * \*/
object TopNGroupStatic {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDataStream: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    //主要key，用来进行keyby操作
    val mainKeyPos="2|3|4".split('|').map(_.toInt-1)

    //TopN的字段与对应升降规则
    val topNFieldArr = "5#desc#10|6#asc#10".split('|')

    //判断数据类型，判断排序方式，判断topN的数量

    //统计结果时长(秒)
    val timeValid="9999".toLong

    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000

    def fieldIsNull(inputStr:String): Boolean ={
      null==inputStr || inputStr=="null" || inputStr==""
    }


    object descOrdering extends Ordering[Double]{
      override def compare(x: Double, y: Double): Int = {
        if(x>=y)-1 else 1
      }
    }

    class TopNKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {

      private var timeState: ValueState[Long] = _
      private var cacheMapState:MapState[String,java.util.TreeMap[Double,String]]=_

      override def open(parameters: Configuration): Unit = {
        timeState = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
        cacheMapState=getRuntimeContext.getMapState(
          new MapStateDescriptor[String,java.util.TreeMap[Double,String]]("cacheMapState",classOf[String],classOf[java.util.TreeMap[Double,String]])
        )

      }

      override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {

        val inputArr: Array[String] = value.split('|')
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

        //缓存多字段TopN
        for (elem <- topNFieldArr) {

          //参数分解
          val fieldIndex = elem.split('#').head.toInt-1
          val sortType=elem.split('#')(1)
          val topNum: Int = elem.split('#').last.toInt
          val fieldValue=inputArr(fieldIndex).toDouble

          var cacheTreeMap: util.TreeMap[Double, String] = cacheMapState.get(fieldIndex.toString)


          if(cacheTreeMap==null){
            if (sortType=="desc"){
              cacheTreeMap=new util.TreeMap[Double,String](descOrdering)
            }else{
              cacheTreeMap=new util.TreeMap[Double,String]()
            }
          }

          cacheTreeMap.put(fieldValue,value)

          if(cacheTreeMap.size()>topNum){
            cacheTreeMap.pollLastEntry()
          }

          cacheMapState.put(fieldIndex.toString,cacheTreeMap)


          //缓存获取
//          cacheMapState.get(fieldIndex)


          //判断排序规则
//          sortType match {
//            case "desc"=>{
//              object descSortMap extends Ordering[Double] {
//                override def compare(input1: Double, input2: Double): Int = {
//                  val value1 = if(fieldIsNull(input1Arr(fieldIndex))) 0.0 else input1Arr(fieldIndex).toInt
//                  val value2 = if(fieldIsNull(input2Arr(fieldIndex))) 0.0 else input2Arr(fieldIndex).toInt
//                  if(value1<value2) -1 else 1
//                }
//              }
//            }
//          }



        }

      }


    }

    inputDataStream.keyBy(str => {
      val arr: Array[String] = str.split('|')
      var key = ""
      for (ele <- mainKeyPos) {
        key += '_'+arr(ele)
      }
      key
    })
//      .process()








  }

}