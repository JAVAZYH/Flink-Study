package TSY

import java.util
import java.util.Random

import MySource.ReportSource
import MyUtil.TimeUtil
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.mutable




/**
  * 量级统计&内容统计
  */

object CountStatic {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
//    val input_DStream1 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\lol_report")
    val input_DStream1 = env.addSource(new ReportSource)

    /**
      * mainkeyPos：主键字段的统计偏移，按|进行分割
      * fieldPos：需要统计的量级字段偏移
      * isDistinct：量级统计时是否去重
      * contentPos：内容字段的统计偏移
      * fieldSplit：内容字段中的分隔符
      */
    val mainkeyPos="4"
    val contentPos=5-1
    val fieldPos=6-1
    val fieldSplit="+"
    val isDistinct=false



    class MyKeypress extends KeyedProcessFunction[String,String,String] {
      /**
        * countState:[对局id，出现次数]
        * contentState[举报内容1，举报内容2]
        */
      private var countState:MapState[String,Long]=_
      private var contentState:ValueState[mutable.Set[String]]=_
      private var timeState:ValueState[Long]=_


      //初始化状态
      override def open(parameters: Configuration): Unit = {
        countState=getRuntimeContext.getMapState(
          new MapStateDescriptor[String, Long]("countState", classOf[String], classOf[Long])
                    )

        contentState=getRuntimeContext.getState(
                      new ValueStateDescriptor[mutable.Set[String]]("contentState", classOf[mutable.Set[String]])
                    )

        timeState=getRuntimeContext.getState(
                      new ValueStateDescriptor[Long]("timeState", classOf[Long])
                    )

      }

      //处理流中的每个元素
      override def processElement(in: String,
                                  ctx: KeyedProcessFunction[String, String, String]#Context,
                                  out: Collector[String]): Unit = {
        val arr: Array[String] = in.split('|')
        val content: String = arr(contentPos)
        val field: String = arr(fieldPos)

        //如果统计值状态里有这个字段,取出+1，没有设置为1
        if(countState.contains(field)){
          val count: Long = countState.get(field)
          countState.remove(field)
          countState.put(field,count+1)
        }
        else{
          countState.put(field,1)
        }

        //取出字段状态里的值，添加当前流元素后更新
        val contentSet: mutable.Set[String] = contentState.value()
        if (contentSet==null){
          val tmp= mutable.Set[String] ()
          tmp+=content
          contentState.update(tmp)
        }else{
          contentSet+=content
          contentState.update(contentSet)
        }



        //如果定时器之前的值为空，注册定时器，到达今天晚上12点时清空状态数据
        //否则说明之前已经注册过定时器，不需要重复注册
        if(timeState.value()==0||timeState.value().isNaN){
          val ts: Long = ctx.timerService().currentProcessingTime()
          val day = ts / (1000 * 60 * 60 * 24 ) + 1
          // 获取定时器下一次计算时间，-8是因为时区问题
          val timer = day * (1000 * 60 * 60 * 24 )-(1000*60*60*8)

          ctx.timerService().registerProcessingTimeTimer(timer)
          timeState.update(timer)
        }

        //将count状态中之前保存的对局id->数量，把数量取出，然后遍历每个数量
        var countRes=0L
        val countIt: util.Iterator[Long] = countState.values().iterator()
        while (countIt.hasNext) {
          //不去重就使用每个字段对应的数量，去重就+1
          val tmpCount = countIt.next()
          if (isDistinct) {
            countRes += 1
          } else {
            countRes += tmpCount
          }
        }

        //将content状态中所有字段取出拼接为一个字符串输出
        var contentRes=""
        val contentSet2: mutable.Set[String] = contentState.value()
        //把迭代器转为数组
        val arrSet: Array[String] = contentSet2.toArray
        //把数组中的字段拼接为一个字符串，字段之间的分隔符自己指定
        for(ele <- arrSet.indices){
          if (ele==0){
            contentRes+=arrSet(ele)
          }else{
            contentRes+=fieldSplit+arrSet(ele)
          }
        }
        out.collect(in+"|"+countRes+"|"+contentRes)

      }

      //第二天定时器到了，需要清空所有缓存状态
      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        println("定时器到了》》》》》》"+TimeUtil.timestampToString(timestamp))
        countState.clear()
        contentState.clear()
        timeState.clear()
      }
    }


    val result: DataStream[String] = input_DStream1
      .keyBy(str => {
        val arr: Array[String] = str.split('|')
        val mainArr: Array[String] = mainkeyPos.split('|')
        var key=""
        for(ele <- mainArr){
          key+=arr(ele.toInt-1)
        }
        key
        })
        .process(new MyKeypress)







    result.print()

    env.execute()



  }

}
