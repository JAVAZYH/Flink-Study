package TSY.gorup.backUp

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConversions._
import  scala.collection.JavaConverters._
/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * \*/
object ProceeingTimeCountStatic{
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //是否去重
    val is_distinct=if("0"=="1")true else false
    //联合主键位置
    val main_key_pos="2|3"
    //统计字段位置
    val field_pos="4|5"
    val fieldArr: Array[Int] = field_pos.split('|').map(_.toInt-1)


    //统计结果时长(秒)
    val time_valid="20".toLong


    val LOG = LoggerFactory.getLogger(this.getClass)

    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)


    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
      private var fieldCacheListState1: ListState[String] = _
      private var fieldCacheListState2: ListState[String] = _

      override def open(parameters: Configuration): Unit = {
        fieldCacheListState1 = getRuntimeContext.getListState(
          new ListStateDescriptor[String]("fieldCacheListState1", classOf[String])
        )
        fieldCacheListState2 = getRuntimeContext.getListState(
          new ListStateDescriptor[String]("fieldCacheListState2", classOf[String])
        )
      }

      override def processElement(input: String,
                                  ctx: KeyedProcessFunction[String, String, String]#Context,
                                  out: Collector[String]): Unit = {
        val input_arr: Array[String] = input.split('|')
        val timer = ctx.timerService().currentProcessingTime() + (time_valid * 1000)
        ctx.timerService().registerProcessingTimeTimer(timer)
        try {
        fieldArr.length match {
          case 1=>{
            val field1 = input_arr(fieldArr.head)
            val fieldSet: Set[String] = fieldCacheListState1.get().iterator().map(_.split('#').last).toSet
            var counts1=0
            if(null==field1 || field1=="null" || field1==""){
              counts1=fieldSet.size
            }else{
              fieldCacheListState1.add(timer+"#"+field1)
              counts1=fieldSet.size+1
            }
            out.collect(input+"|"+counts1)
        }
          case 2=>{
            val field1=input_arr(fieldArr.head)
            val field2=input_arr(fieldArr.last)

            val fieldSet: Set[String] = fieldCacheListState1.get().iterator().map(_.split('#').last).toSet
            val fieldSet2: Set[String] = fieldCacheListState2.get().iterator().map(_.split('#').last).toSet
            var counts1=0
            var counts2=0
            if(null==field1 || field1=="null" || field1==""){
              counts1=fieldSet.size
            }else{
              fieldCacheListState1.add(timer+"#"+field1)
              counts1=fieldSet.size+1
            }
            if(null==field2 || field2=="null" || field2==""){
              counts2=fieldSet2.size
            }else{
              fieldCacheListState2.add(timer+"#"+field2)
              counts2=fieldSet2.size+1
            }
            out.collect(input+"|"+counts1+"|"+counts2)
          }
        }
        }catch {
          case exception: Exception=>{
            LOG.error(input+"出错了"+exception.getMessage)
          }
        }
      }

      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        if (fieldCacheListState1 != null) {
          val resultList = new util.LinkedList[String]()
          println("清理前的list是" + fieldCacheListState1.get().asScala.toList)
          val it: util.Iterator[String] = fieldCacheListState1.get().iterator()
          while (it.hasNext) {
            val next = it.next()
            val registerTimer = next.split('#').head.toLong
            if (registerTimer != timestamp) {
              resultList.add(next)
            }
          }
          fieldCacheListState1.update(resultList)
          println("清理后的list是" + resultList)
        }
        if (fieldArr.length==2){
          if (fieldCacheListState2 != null) {
            val resultList2 = new util.LinkedList[String]()
            println("清理前的list2是" + fieldCacheListState2.get().asScala.toList)
            val it2: util.Iterator[String] = fieldCacheListState2.get().iterator()
            while (it2.hasNext) {
              val next = it2.next()
              val registerTimer = next.split('#').head.toLong
              if (registerTimer != timestamp) {
                resultList2.add(next)
              }
            }
            fieldCacheListState2.update(resultList2)
            println("清理后的list2是" + resultList2)
          }
        }
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
      .uid("processTimeCount")

    resultDS.print()
    env.execute()
  }


}