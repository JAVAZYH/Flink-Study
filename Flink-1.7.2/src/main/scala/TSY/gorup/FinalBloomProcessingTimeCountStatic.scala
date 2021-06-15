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
object FinalBloomProcessingTimeCountStatic{
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    //主要key，用来进行keyby操作，key不能太多并且需要保持散列，默认会根据第一个位置当作游戏id创建布隆过滤器大小

    val inputMainKeyPos="4|5|6|7|8".split('|').map(_.toInt-1)

    val mainKeyPos=inputMainKeyPos.dropRight(1)

    //联合主键位置
    val otherKeyPos=inputMainKeyPos.takeRight(1)

    //统计字段位置
    val staticFieldArr="1|9|10".split('|').map(_.toInt-1)

    //统计结果时长(秒)
    val timeValid="9999".toLong

    val keyCounts="600".toInt
    val dataCounts="90000".toLong
    val keybyCoutns=((dataCounts/keyCounts)*10000).toInt

    val fpp=1E-4

    val LOG = LoggerFactory.getLogger(this.getClass)

    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000


    def fieldIsNull(inputStr:String): Boolean ={
      null==inputStr || inputStr=="null" || inputStr==""
    }

    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
      private var bloomValueState:ValueState[BloomFilter[String]]=_
      private var countMapState:MapState[String,Long]=_
      private var timeState: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        bloomValueState=getRuntimeContext.getState(
          new ValueStateDescriptor[BloomFilter[String]](
            "bloomValueState",classOf[BloomFilter[String]]
          )
        )
        countMapState=getRuntimeContext.getMapState(
          new MapStateDescriptor[String,Long]
          ("countMapState",classOf[String],classOf[Long])
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

        //构建副key
        var otherKey=""
        for (ele <- otherKeyPos) {
          otherKey +=inputArr(ele)+"#"
        }
        val startTime=System.currentTimeMillis()

        val resultArr: ArrayBuffer[Long] = ArrayBuffer[Long]()
        try {
          for (fieldIndex <- staticFieldArr) {
            //用主key进行keyby，副key加到统计字段上进行去重统计，每个keyby维护一个全局布隆过滤器和多个量级缓存器

            var countBloom= bloomValueState.value()

            val field=inputArr(fieldIndex)

            val countsKey=otherKey+fieldIndex

            val keyField=countsKey+"#"+field


            var counts=countMapState.get(countsKey)
            //更新状态和布隆
            if (!fieldIsNull(field)){
            if (countBloom == null) {
              countBloom = BloomFilter.create(Funnels.unencodedCharsFunnel(), keybyCoutns, fpp)
            }
            if (!countBloom.mightContain(keyField)){
              counts+=1
              countMapState.put(countsKey,counts)
              countBloom.put(keyField)
              bloomValueState.update(countBloom)
            }

            }
            resultArr.append(counts)
          }

          if(System.currentTimeMillis - startTime>10){
            LOG.info(s"""${ctx.getCurrentKey}代码执行总共耗时为:${System.currentTimeMillis - startTime }ms""")
          }
          out.collect(input+"|"+resultArr.mkString("|"))

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
//        print(s"""定时触发了，触发的key是${ctx.getCurrentKey},触发的时间是${timestamp}""")
        timeState.clear()
        countMapState.clear()
        bloomValueState.clear()
      }


    }

    val resultDS: DataStream[String] = input_DStream1
      .keyBy(str => {
        val inputArr: Array[String] = str.split('|')
        var key = ""
        for (ele <- mainKeyPos) {
          key += '_'+inputArr(ele.toInt)
        }
        key
      })
      .process(new CountStaticKeyedProcessFunction)
      .uid("processing_bloom_count_static")

    resultDS.print()
    env.execute()
  }


}