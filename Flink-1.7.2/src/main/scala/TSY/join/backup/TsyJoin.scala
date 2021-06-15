package TSY.join.backup

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/12/4
  * \* Time: 9:43
  * \*/
//目前先提供指定时间范围，全连接
//给每条缓存的数据都加上 前缀，前缀为过期的时间戳，等到定时器触发就把这些数据删除

object TsyJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)


    val inColumnLists=List(List(List("log_time", "STRING"), List("game_id", "BIGINT"),List("uin", "BIGINT")),
      List(List("log_time2", "STRING"), List("game_id", "BIGINT"),List("account", "BIGINT")))
    /**
      * 将列名解析为索引
      * @param parseStr 待解析字符串
      * @param inputList 列名集合
      * @return 索引字符串
      */
    def parseColumnsName(parseStr:String,inputList:List[List[String]]):String={
      //解析列名
      val inputColumnsArr=ArrayBuffer[String]()
      for (elem <- inputList) {
        val columnName: String = elem.head
        inputColumnsArr.append(columnName)
      }
      //解析索引
      val IndexArr=ArrayBuffer[Int]()
      for (columnName <- parseStr.split(',')) {
          //+1是为了兼容旧代码
          val index: Int = inputColumnsArr.indexOf(columnName)+1
          if(index==0){
            return  null
          }
          IndexArr.append(index)
        }
      IndexArr.mkString(",")
    }
    val inputMainKey="game_id,uin|log_time2,account"
    val input1IndexStr = parseColumnsName(inputMainKey.split('|').head,inColumnLists.head)
    val input2IndexStr = parseColumnsName(inputMainKey.split('|').last,inColumnLists.last)

    val mainKey= input1IndexStr+"|"+input2IndexStr

    val joinType="left_join"
    val timeValid="9999".toLong
    val leftStreamLength="3".toInt
    val rightStreamLength="3".toInt
    
    

    val LOG=LoggerFactory.getLogger(this.getClass)

    println(s"""从allblue接收到的参数为：$mainKey == $joinType == $timeValid == $leftStreamLength == $rightStreamLength""")
    LOG.info(s"""从allblue接收到的参数为：$mainKey == $joinType == $timeValid == $leftStreamLength == $rightStreamLength""")


    def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
      val timeFormat = new SimpleDateFormat(format)
      val timestamp = timeFormat.parse(timeStr).getTime
      timestamp
    }

    def timestampToString(timestamp: Long, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
      val timeFormat = new SimpleDateFormat(format)
      val timeStr = timeFormat.format(new Date(timestamp))
      timeStr
    }

    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000
    /**
      * 获取定时器触发时间戳（毫秒）
      * @param currentTS 系统当前时间戳（毫秒）
      * @param timeValid 数据有效期，9999表示当天
      * @return
      */
    def getTimerTS(currentTS:Long,timeValid: Long): Long ={
      var resTS=0L
      if (timeValid == 9999L) {
          val tomorrowTS = tomorrowZeroTimestampMs(currentTS, 8)
          resTS=tomorrowTS
      }
      else {
        resTS = currentTS + (timeValid * 1000)
      }
      resTS
    }


    

    val inputDS1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)


    val inputDS2: DataStream[String] = env.socketTextStream("9.134.217.5",9998)

    val keyedStream1: KeyedStream[String, String] = inputDS1
      .keyBy(str=>{
      val mainKey1: String = mainKey.split('|').head
      val inputArr: Array[String] = str.split('|')
      val mainArr: Array[String] = mainKey1.split(',')
      var key = ""
      for (ele <- mainArr) {
        key += "_"+inputArr(ele.toInt - 1)
      }
      key
    })

    val keyedStream2: KeyedStream[String, String] = inputDS2
      .keyBy(str=>{
        val mainKey2: String = mainKey.split('|').last
        val inputArr: Array[String] = str.split('|')
        val mainArr: Array[String] = mainKey2.split(',')
        var key = ""
        for (ele <- mainArr) {
          key += "_"+inputArr(ele.toInt - 1)
        }
        key
      })

    class JoinCoProcessFunction  extends CoProcessFunction[String,String,String] {

      private var streamCacheListState1:ListState[String]=_
      private var streamCacheListState2:ListState[String]=_
      private var timeState: ValueState[Long] = _


      override def open(parameters: Configuration): Unit = {
        streamCacheListState1=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("streamCacheListState1",classOf[String])
        )
        streamCacheListState2=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("streamCacheListState2",classOf[String])
        )
        timeState = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
      }

      override def processElement1(value: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        //左连接一流无需缓存，直接关联输出
        val nullStreamStr2 = new Array[String](rightStreamLength).mkString("|")
        if(joinType=="left_join"){
          val streamList2: List[String] = streamCacheListState2.get().iterator().asScala.toList
          if(streamList2.nonEmpty){
            for (elem <- streamList2) {
              val input2: String = elem.split('#').last
              out.collect(value +"|"+input2)
            }
          }else{
            out.collect(value+"|"+nullStreamStr2)
          }
        }else {
          val mainKey1: String = mainKey.split('|').head
          val inputArr: Array[String] = value.split('|')
          val mainArr: Array[String] = mainKey1.split(',')
          var key = ""
          for (ele <- mainArr) {
            key += "_"+inputArr(ele.toInt - 1)
          }

          val ts=ctx.timerService().currentProcessingTime()
          var timer = 0L
          if (timeValid == 9999L) {
            if (timeState.value() == 0L) {
              val tomorrowTS = tomorrowZeroTimestampMs(ts, 8)
              ctx.timerService().registerProcessingTimeTimer(tomorrowTS)
              timer = tomorrowTS
              timeState.update(tomorrowTS)
            }
          }
          else {
            timer = ts + (timeValid * 1000)
            ctx.timerService().registerProcessingTimeTimer(timer)
          }

          streamCacheListState1.add(timer + "#" + value)
          val streamList2: List[String] = streamCacheListState2.get().iterator().asScala.toList

          if (streamList2.nonEmpty) {
            for (elem <- streamList2) {
              val input2: String = elem.split('#').last
              out.collect(value +"|"+input2)
            }
          }
          else{
            //如果是内连接不输出一流未关联到的数据
            if(joinType=="full_join") {
              out.collect(value+"|"+nullStreamStr2)
            }
          }

        }

      }

      override def processElement2(value: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        val nullStreamStr1 = new Array[String](leftStreamLength).mkString("|")
        val mainKey2: String = mainKey.split('|').last
        val inputArr: Array[String] = value.split('|')
        val mainArr: Array[String] = mainKey2.split(',')
        var key = ""
        for (ele <- mainArr) {
          key += "_"+inputArr(ele.toInt - 1)
        }

        val ts=ctx.timerService().currentProcessingTime()
        var timer=0L
        if(timeValid==9999L){
          if(timeState.value()==0){
            val tomorrowTS=tomorrowZeroTimestampMs(ts,8)
            ctx.timerService().registerProcessingTimeTimer(tomorrowTS)
            timer=tomorrowTS
            timeState.update(tomorrowTS)
          }
        }
        else{
          timer=ts+(timeValid*1000)
          ctx.timerService().registerProcessingTimeTimer(timer)
        }
        streamCacheListState2.add(timer+"#"+value)

        //当不为左连接的时候输出二流
        if(joinType!="left_join"){
          
          import scala.collection.JavaConverters._
          val streamList1: List[String] = streamCacheListState1.get().iterator().asScala.toList
          if(streamList1.nonEmpty){
            for (elem <- streamList1) {
              val input1: String = elem.split('#').last
              out.collect(input1 +"|"+value)
            }
          }else{
            //如果是全连接输出1流空数据
            if(joinType=="full_join") {
              out.collect(nullStreamStr1+"|"+value)
            }
          }
        }

      }

      override def onTimer(timestamp: Long, ctx: CoProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
        timeState.clear()
        if(streamCacheListState1!=null){
          if (timeValid==9999L){
            streamCacheListState1.clear()
          } else {
            //循环清除到期数据
            val resultList= new util.LinkedList[String]()
            val it: util.Iterator[String] =streamCacheListState1.get().iterator()
            while(it.hasNext){
              val next=it.next()
              val registerTimer=next.split('#').head.toLong
              if(registerTimer!=timestamp){
                resultList.add(next)
              }
            }
            streamCacheListState1.update( resultList)
          }
        }

        if(streamCacheListState2!=null){
          if(timeValid==9999L){
            streamCacheListState2.clear()
          }else{
            //循环清除到期数据
            val resultList= new util.LinkedList[String]()
            val it2: util.Iterator[String] =streamCacheListState2.get().iterator()
            while(it2.hasNext){
              val next=it2.next()
              val registerTimer=next.split('#').head.toLong
              if(registerTimer!=timestamp){
                resultList.add(next)
              }
            }
            streamCacheListState2.update( resultList)
          }
        }
      }
    }

    val resultDS: DataStream[String] = keyedStream1.connect(keyedStream2)
      .process(new JoinCoProcessFunction)


    resultDS.print()
    env.execute()
  }

}