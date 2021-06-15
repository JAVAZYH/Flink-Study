package TSY.gorup.backUp

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * \*/
object FinalProcessingTimeCountStatic{
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    //联合主键位置
    val main_key_pos="1"

    //统计字段位置
    val field_pos="2|3"

    val sum_field="3"

    val fieldArr: Array[Int] = field_pos.split('|').map(_.toInt-1)


    //统计结果时长(秒)
    val time_valid="9999".toLong


    val LOG = LoggerFactory.getLogger(this.getClass)

    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000


    def fieldIsNull(inputStr:String): Boolean ={
      null==inputStr || inputStr=="null" || inputStr==""
    }

    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
      private  var cacheValueState1:ValueState[mutable.Set[String]]=_
      private  var cacheValueState2:ValueState[mutable.Set[String]]=_
      private  var cacheValueState3:ValueState[mutable.Set[String]]=_
      private var timeState: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {

        cacheValueState1=getRuntimeContext.getState(
          new ValueStateDescriptor[mutable.Set[String]](
            "cacheValueState1",classOf[mutable.Set[String]]
          )
        )

        cacheValueState2=getRuntimeContext.getState(
          new ValueStateDescriptor[mutable.Set[String]](
            "cacheValueState2",classOf[mutable.Set[String]]
          )
        )

        cacheValueState3=getRuntimeContext.getState(
          new ValueStateDescriptor[mutable.Set[String]](
            "cacheValueState3",classOf[mutable.Set[String]]
          )
        )

        timeState = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )

      }

      override def processElement(input: String,
                                  ctx: KeyedProcessFunction[String, String, String]#Context,
                                  out: Collector[String]): Unit = {
        val input_arr: Array[String] = input.split('|')

        def countsStatic(field:String,inputValueState:ValueState[mutable.Set[String]]) ={
          var fieldSet: mutable.Set[String]=null
          if(inputValueState.value()==null)fieldSet=mutable.Set[String]()else fieldSet=inputValueState.value()
          var counts=0
          if(fieldIsNull(field)){
            counts=fieldSet.size
          }else{
            fieldSet.add(field)
            counts=fieldSet.size
            inputValueState.update(fieldSet)
          }
          counts
        }


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

        try {
        fieldArr.length match {
          case 1=>{
            val field1: String =input_arr(fieldArr(0))
            val counts1: Int = countsStatic(field1,cacheValueState1)

            out.collect(input+"|"+counts1)
        }
          case 2=>{
            val field1: String =input_arr(fieldArr(0))
            val field2: String =input_arr(fieldArr(1))
            val counts1: Int = countsStatic(field1,cacheValueState1)
            val counts2: Int = countsStatic(field2,cacheValueState2)
            out.collect(input+"|"+counts1+"|"+counts2)
          }
          case 3=>{
            val field1: String =input_arr(fieldArr(0))
            val field2: String =input_arr(fieldArr(1))
            val field3: String =input_arr(fieldArr(2))
            val counts1: Int = countsStatic(field1,cacheValueState1)
            val counts2: Int = countsStatic(field2,cacheValueState2)
            val counts3: Int = countsStatic(field3,cacheValueState3)
            out.collect(input+"|"+counts1+"|"+counts2+"|"+counts3)
          }
        }
        }catch {
          case exception: Exception=>{
            LOG.error(input+"出错了"+exception.getMessage)
            throw exception
          }
        }
      }

      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        LOG.info(s"""定时触发了，触发的key是${ctx.getCurrentKey},触发的时间是${timestamp}""")
        print(s"""定时触发了，触发的key是${ctx.getCurrentKey},触发的时间是${timestamp}""")
        timeState.clear()
        cacheValueState1.clear()
        cacheValueState2.clear()
        cacheValueState3.clear()
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
      .uid("processing_time_count_static")

    resultDS.print()
    env.execute()
  }


}