package TSY.join

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
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


    val inColumnLists=
      List(List(List("log_time", "STRING"), List("game_id", "BIGINT"),List("uin", "BIGINT"),List("hash","STRING")),
      List(List("log_time", "STRING"), List("game_id", "BIGINT"),List("account", "BIGINT"),List("time_stamp", "BIGINT")))
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

    val inputMainKey="game_id,uin|game_id,account"
    val joinType="inner_join"
    val streamValid1="30".toLong
    val streamValid2="30".toLong


    //获取字符串长度
    val input1IndexStr = parseColumnsName(inputMainKey.split('|').head,inColumnLists.head)
    val input2IndexStr = parseColumnsName(inputMainKey.split('|').last,inColumnLists.last)
    val mainKey= input1IndexStr+"|"+input2IndexStr
    //获取左右流的长度用于补齐字段
    val streamLength1=inColumnLists.head.length
    val streamLength2=inColumnLists.last.length


    val LOG=LoggerFactory.getLogger(this.getClass)

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
      * @param timeValid 数据有效期(秒)，9999表示当天
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

      private var streamListState1:ListState[String]=_
      private var streamListState2:ListState[String]=_
      private var timeState1: ValueState[Long] = _
      private var timeState2: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        streamListState1=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("streamListState1",classOf[String])
        )
        streamListState2=getRuntimeContext.getListState(
          new ListStateDescriptor[String]("streamListState2",classOf[String])
        )
        timeState1 = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState1", classOf[Long])
        )
        timeState2 = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState2", classOf[Long])
        )
      }

      override def processElement1(value: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {


        val nullStreamStr2 = new Array[String](streamLength2).mkString("|")

        //缓存数据
        if(streamValid1!=0L){
        if(timeState1.value()==0L){
          val ts=ctx.timerService().currentProcessingTime()
          var timer = 0L
          //注册当天定时器
          if (streamValid1 == 9999L) {
              val tomorrowTS = tomorrowZeroTimestampMs(ts, 8)
              ctx.timerService().registerProcessingTimeTimer(tomorrowTS)
              timer = tomorrowTS
              timeState1.update(tomorrowTS)
          }
          else {
            timer = ts + (streamValid1 * 1000)
            ctx.timerService().registerProcessingTimeTimer(timer)
          }
        }
        streamListState1.add(value)
        }


        //关联数据
        if(joinType=="left_join"){
          val streamList2: List[String] = streamListState2.get().iterator().asScala.toList
          if(streamList2.nonEmpty){
            for (elem <- streamList2) {
              out.collect(value +"|"+elem)
            }
          }else{
            out.collect(value+"|"+nullStreamStr2)
          }
        }
        else {//内连接或全连接
          val streamList2: List[String] = streamListState2.get().iterator().asScala.toList

          if (streamList2.nonEmpty) {
            for (elem <- streamList2) {
              out.collect(value +"|"+elem)
            }
          }
          else{
            //如果是全连接补空值输出
            if(joinType=="full_join") {
              out.collect(value+"|"+nullStreamStr2)
            }
          }

        }

      }

      override def processElement2(value: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {

        //缓存数据
        val nullStreamStr1 = new Array[String](streamLength1).mkString("|")

        if(streamValid2!=0L){
        if(timeState2.value()==0L){
        val ts=ctx.timerService().currentProcessingTime()
        var timer=0L
        if(streamValid2==9999L){
            val tomorrowTS=tomorrowZeroTimestampMs(ts,8)
            ctx.timerService().registerProcessingTimeTimer(tomorrowTS)
            timer=tomorrowTS
            timeState1.update(tomorrowTS)
        }
        else{
          timer=ts+(streamValid2*1000)
          ctx.timerService().registerProcessingTimeTimer(timer)
        }
        }
        streamListState2.add(value)
        }

        //关联数据，当不为左连接的时候输出二流
        if(joinType!="left_join"){
          import  scala.collection.JavaConverters._
          val streamList1: List[String] = streamListState1.get().iterator().asScala.toList
          if(streamList1.nonEmpty){
            for (elem <- streamList1) {
              out.collect(elem +"|"+value)
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
        timeState1.clear()
        streamListState1.clear()
        streamListState2.clear()

      }
    }

    val resultDS: DataStream[String] = keyedStream1.connect(keyedStream2)
      .process(new JoinCoProcessFunction)


    resultDS.print()
    env.execute()
  }

}