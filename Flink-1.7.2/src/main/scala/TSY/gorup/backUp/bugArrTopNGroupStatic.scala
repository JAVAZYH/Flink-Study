package TSY.gorup.backUp

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2021/2/1
  * \* Time: 14:31
  * TopN统计,每次当排行榜有更新时，输出需要更新的数据，需要配合下游ES去重使用
  * \*/
object newTopNGroupStatic {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDataStream: DataStream[String] = env.socketTextStream("9.134.217.5",9999)



    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000

    /**
      * 将列名解析为索引
      *
      * @param parseStr  待解析字符串
      * @param inputList 列名集合
      * @return 索引字符串
      */
    def parseColumnsName(parseStr: String, inputList: List[List[String]]): String = {
      //解析列名
      val inputColumnsArr = ArrayBuffer[String]()
      for (elem <- inputList) {
        val columnName: String = elem.head
        inputColumnsArr.append(columnName)
      }
      //解析索引
      val IndexArr = ArrayBuffer[Int]()
      for (columnName <- parseStr.split(',')) {
        //+1是为了兼容旧代码
        val index: Int = inputColumnsArr.indexOf(columnName) + 1
        if (index == 0) {
          return null
        }
        IndexArr.append(index)
      }
      IndexArr.mkString(",")
    }

    val inColumnLists = List(
      List(List("log_time", "STRING"), List("uin", "STRING"), List("policy_id", "BIGINT"), List("punish_time", "BIGINT"))
    )

    //主要key，用来进行keyby操作
    val mainKeyPos = "uin|policy_id".split('|').map(str => {
      parseColumnsName(str, inColumnLists.head).toInt - 1
    })

    //统计结果时长(秒)
    val timeValid = "9999".toLong
    val sortType = "DESC_SORT"
    val topNumber = "3".toInt
    val isDistinct = "0" match {
      case "1" => true
      case _ => false
    }
    //排序字段
    val orderFieldIndex = parseColumnsName("punish_time", inColumnLists.head).toInt - 1


    class TopNKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {

      private var timeState: ValueState[Long] = _
      private var topNValueState: ValueState[ArrayBuffer[(Double,String)]]=_

      override def open(parameters: Configuration): Unit = {
        timeState = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
        topNValueState = getRuntimeContext.getState(
          new ValueStateDescriptor[ArrayBuffer[(Double,String)]]("topNValueState", classOf[ArrayBuffer[(Double,String)]])
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


        //参数分解

        try {
          val orderField = inputArr(orderFieldIndex).toDouble

          var cacheArrBuff: ArrayBuffer[(Double, String)] = topNValueState.value()
          //构建treeMap
          if (cacheArrBuff == null) {
            cacheArrBuff = ArrayBuffer[(Double, String)]()
          }
          val orgArrBuff: ArrayBuffer[(Double, String)] = cacheArrBuff.clone()

          cacheArrBuff.append(orderField->value)

          if(sortType=="DESC_SORT"){
            cacheArrBuff=cacheArrBuff.sorted(Ordering.Tuple2(Ordering.Double.reverse,Ordering.String))
          }else{
            cacheArrBuff=cacheArrBuff.sorted(Ordering.Tuple2(Ordering.Double,Ordering.String))
          }

          if (cacheArrBuff.size > topNumber) {
            cacheArrBuff = cacheArrBuff.dropRight(1)
          }
          topNValueState.update(cacheArrBuff)

          if(!cacheArrBuff.equals(orgArrBuff)){
            val diffArr: ArrayBuffer[(Double, String)] = cacheArrBuff.diff(orgArrBuff)
            if(diffArr!=null){
              val diffElem: (Double, String) = diffArr.head
              val diffElemIndex: Int = cacheArrBuff.lastIndexOf(diffElem)
              //取出有变动的数据，并且更新排名
              val outUpdateArr = cacheArrBuff.takeRight(cacheArrBuff.length - diffElemIndex)
              for (elem <- outUpdateArr) {
                out.collect(elem._2+"|"+(cacheArrBuff.lastIndexOf(elem)+1))
              }
            }
          }

        }catch {
          case exception: Exception=>{
            throw exception
          }
        }

      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
        timeState.clear()
        topNValueState.clear()
      }
    }

    val resultDS: DataStream[String] = inputDataStream.keyBy(str => {
      val arr: Array[String] = str.split('|')
      var key = ""
      for (ele <- mainKeyPos) {
        key += '_' + arr(ele)
      }
      key
    })
      .process(new TopNKeyedProcessFunction)

    resultDS.print()



    env.execute()

  }

}