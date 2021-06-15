package TSY.gorup

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import scala.collection.mutable.ArrayBuffer

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/10/21
  * \* Time: 9:31
  * 通过布隆过滤器指定key，进行去重统计
  * 游戏id会作为keyby的条件，keyby之后取到的状态都是该key的
  * 不同的key拿到的状态不同
  * 不采用valuestate存储布隆过滤器的原因是key过多内存不够用
  *
  * keyby之后维护一个全局的布隆过滤器
  * \*/
object UinRandomBloomProcessingTimeCountStatic{
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    //主要key，用来进行keyby操作，key不能太多并且需要保持散列，默认会根据第一个位置当作游戏id创建布隆过滤器大小
    val mainKeyPos="2".split('|').map(_.toInt-1)
    //联合主键位置
    val otherKeyPos="3".split('|').map(_.toInt-1)
    //统计字段位置
    val staticFieldArr="1|4".split('|').map(_.toInt-1)

    //统计结果时长(秒)
    val timeValid="9999".toLong

    val keyCounts="600".toInt
    val dataCounts="90000".toInt*10000
    val keybyCoutns=dataCounts/keyCounts
    val fpp=1E-4

    val uinPos="1".toInt-1

    val isLoadBalance="1" match{
      case "0"=>false
      case "1"=>true
    }


    val LOG = LoggerFactory.getLogger(this.getClass)

    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000

    def fieldIsNull(inputStr:String): Boolean ={
      null==inputStr || inputStr=="null" || inputStr==""
    }
    def calStartTime(): Long ={
      System.currentTimeMillis()
    }

    def calTotalTime(startTime:Long): String ={
      s"""该程序代码块执行总共耗时为:${(System.currentTimeMillis - startTime) / 1000.0}"""
    }

    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
      private var bloomValueState:ValueState[BloomFilter[String]]=_
      private var uinPartitionMapState:MapState[String,Long]=_
      private var timeState: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        bloomValueState=getRuntimeContext.getState(
          new ValueStateDescriptor[BloomFilter[String]](
            "bloomValueState",classOf[BloomFilter[String]]
          )
        )
        uinPartitionMapState=getRuntimeContext.getMapState(
          new MapStateDescriptor[String,Long]
          ("uinPartitionMapState",classOf[String],classOf[Long])
        )
        timeState = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
      }

      override def processElement(input: String,
                                  ctx: KeyedProcessFunction[String, String, String]#Context,
                                  out: Collector[String]): Unit = {
        val inputArr: Array[String] = input.split('|')
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

        val startTime: Long = calStartTime()

        //构建副key
        var otherKey=""
        for (ele <- otherKeyPos) {
          otherKey +=inputArr(ele)+"#"
        }

        val resultArr: ArrayBuffer[Long] = ArrayBuffer[Long]()
        try {
          for (fieldIndex <- staticFieldArr) {
            //用主key进行keyby，副key加到统计字段上进行去重统计，每个keyby维护一个全局布隆过滤器和多个量级缓存器

            var countBloom= bloomValueState.value()

            val field=inputArr(fieldIndex)

            val countsKey=otherKey+fieldIndex

            val keyField=countsKey+"#"+field

            var counts=uinPartitionMapState.get(countsKey)

            //更新状态和布隆
            if (!fieldIsNull(field)){
            if (countBloom == null) {
              countBloom = BloomFilter.create(Funnels.unencodedCharsFunnel(), keybyCoutns, fpp)
            }
            if (!countBloom.mightContain(keyField)){
              counts+=1
              uinPartitionMapState.put(countsKey,counts)
              countBloom.put(keyField)
              bloomValueState.update(countBloom)
            }
            }
            resultArr.append(counts)
          }
          if(isLoadBalance){
            out.collect(ctx.getCurrentKey+"|"+input+"|"+resultArr.mkString("|"))
          }else{
            out.collect(input+"|"+resultArr.mkString("|"))
          }
          println(s"""key:${ctx.getCurrentKey}+value:${input}+${calTotalTime(startTime)}""")

          LOG.info(s"""key:${ctx.getCurrentKey}+value:${input}+${calTotalTime(startTime)}""")

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
        timeState.clear()
        uinPartitionMapState.clear()
        bloomValueState.clear()
      }
    }

    //汇总来自不同分区的数据
    class sumStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String]{
      private var timeState: ValueState[Long] = _
      private var uinPartitionMapState:MapState[String,Long]=_
      private var totalCountsMapState:MapState[String,Long]=_



      override def open(parameters: Configuration): Unit = {
        timeState = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("timeState", classOf[Long])
        )
        //存储每个分区的多个统计量级
        uinPartitionMapState=getRuntimeContext.getMapState(
          new MapStateDescriptor[String,Long]
          ("uinPartitionMapState",classOf[String],classOf[Long])
        )
        totalCountsMapState=getRuntimeContext.getMapState(
          new MapStateDescriptor[String,Long]
          ("totalCountsMapState",classOf[String],classOf[Long])
        )
        
      }

      override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {

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

        val inputArr: Array[String] = value.split('|').drop(1)
        val resultArr: ArrayBuffer[Long] = ArrayBuffer[Long]()
        val staticArrLength: Int = staticFieldArr.length

        val currentUinPartition=value.split('|').head.split('_').last
        //遍历统计字段索引
        for (index <- staticArrLength to 1 by -1) {

          //构建分区索引key
          val currentUinPartitionIndex=currentUinPartition+"-"+index

          //取到该数据流中对应索引的统计结果
          val curParCounts=inputArr(inputArr.length-index).toLong

          //获取到当前分区当前索引的缓存结果
          val parCacheCounts: Long = uinPartitionMapState.get(currentUinPartitionIndex)

          //获取到当前索引的总缓存结果
          var totalCacheCounts=totalCountsMapState.get(index.toString)

          totalCacheCounts=totalCacheCounts-parCacheCounts+curParCounts

          //更新状态
          totalCountsMapState.put(index.toString,totalCacheCounts)
          uinPartitionMapState.put(currentUinPartitionIndex,curParCounts)

          resultArr.append(totalCacheCounts)

        }
        out.collect(inputArr.dropRight(staticArrLength).mkString("|")+"|"+resultArr.mkString("|"))

      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
        LOG.info(s"""定时触发了，触发的key是${ctx.getCurrentKey},触发的时间是${timestamp}""")
        timeState.clear()
        uinPartitionMapState.clear()
        totalCountsMapState.clear()
      }


    }


    val keyedProcessDS: DataStream[String] = input_DStream1
      .keyBy(str => {
        if(isLoadBalance) {
          val inputArr: Array[String] = str.split('|')
          val uin =inputArr(uinPos)
          println(uin.substring(uin.length - 2, uin.length))
          var key = ""
          for (ele <- mainKeyPos) {
            key += inputArr(ele.toInt) + "_"
          }
          key + uin.substring(uin.length-2, uin.length)
        }else {
          val inputArr: Array[String] = str.split('|')
          var key = ""
          for (ele <- mainKeyPos) {
            key += inputArr(ele.toInt)
          }
          key
        }
      })
      .process(new CountStaticKeyedProcessFunction)
      .uid("subPartition_bloom_count_static")

    var resultDS: DataStream[String]=null
    if(isLoadBalance){
      resultDS = keyedProcessDS
        .keyBy(str => {
          val streamKey: String = str.split('|').head
          val key: Array[String] = streamKey.split('_').dropRight(1)
          key.mkString("_")
        })
        .process(new sumStaticKeyedProcessFunction)
        .uid("sum_bloom_count_static")
    }else{
      resultDS=keyedProcessDS
    }


    resultDS.print()
    env.execute()


  }


}