package TSY

import java.text.SimpleDateFormat
import java.util.Random

import MySource.DubiousSource
import MyUtil.{CollectionUtil, TimeUtil}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{TimeCharacteristic, scala}
import org.apache.flink.streaming.api.scala.{KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction




case class report(
                   var reporttime:String,
                   var reportuin:String,
                   var reporteduin:String
                 )
case class dubious(

                    var uin:String,
                    var dubioustime:String
                  )



object ConnectJoin {
  def main(args: Array[String]): Unit = {
    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //    val input_Dstream1 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\dubious")
    val input_Dstream1 = env.addSource(new DubiousSource)
    val input_Dstream2 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\report")




    /**
      * mainkeyPos1 第一条流的主键偏移，多个主键按|分隔
      * mainkeyPos2 第二条流的主键偏移，多个主键按|分隔
      */
    val  mainkeyPos1="1"
    val  mainkeyPos2="3"

    /**
      * timeValid1 第一条流的延迟计算时间
      * timeValid2 第二条流的缓存时间
      */
    val  timeValid1=0 * 1000L
    val  timeValid2=10 * 1000L


    //第一条流
    val keyed_DStream1: KeyedStream[String, String] = input_Dstream1
      .keyBy(str => {
        val arr: Array[String] = str.split('|')
        val mainArr: Array[String] = mainkeyPos1.split('|')
        var key=""
        for(ele <- mainArr){
          key+=arr(ele.toInt-1)
        }
        key
      })

    //第二条流
    val keyed_DStream2: KeyedStream[String, String] = input_Dstream2
      .keyBy(str => {
        val arr: Array[String] = str.split('|')
        val mainArr: Array[String] = mainkeyPos2.split('|')
        var key=""
        for(ele <- mainArr){
          key+=arr(ele.toInt-1)
        }
        key
      })

    //自定义process函数
    class MyProcessFunction extends CoProcessFunction[String, String, String] {


      /**
        * ListData1 缓存第一条流数据
        * ListData2 缓存第二条流数据
        * in2Timer 缓存第二条流定时器值
        */
      private var ListData1: ListState[String] = _
      private var ListData2: ListState[String] = _
      private var in2Timer: ValueState[Long] = _


      //初始化状态
      override def open(parameters: Configuration): Unit = {
        ListData1 = getRuntimeContext.getListState(
          new ListStateDescriptor[String]("ListData1", classOf[String])
        )
        ListData2 = getRuntimeContext.getListState(
          new ListStateDescriptor[String]("ListData2", classOf[String])
        )
        in2Timer = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("in1Timer", classOf[Long])
        )

      }


      override def processElement1(in1: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        /**
          * 如果第一条流的数据到达，判断是否需要延迟计算
          * 让数据进行等待，等待到某个时间之后，触发计算
          */
        if (timeValid1 == 0) {
          //如果不需要延迟计算，取出第二条流中的缓存数据
          val it2 = ListData2.get.iterator()
          if (!it2.hasNext) {
            //如果第二条流没有数据，则只输出第一条流的数据
            out.collect(in1)
          }
          else {
            //如果第一条流和第二条流都有数据，则把两条流的数据合并输出
            while (it2.hasNext) {
              out.collect(in1+"|"+it2.next())
            }
          }
        }

        //如果需要延迟计算
        else
        {
          //保存第一条流的数据缓存到list中，并且注册定时器
          ListData1.add(in1)
          val timer = ctx.timerService().currentProcessingTime() + timeValid1
          ctx.timerService().registerProcessingTimeTimer(timer)
        }

      }

      override def processElement2(in2: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        /**
          * 注册一个定时器，在某个时间后清除该缓存，如果之前注册过定时器则不再注册
          *  第二条流的数据到了，都缓存到list中，
          */
        if (in2Timer.value() == 0) {
          val timer = ctx.timerService().currentProcessingTime() + timeValid2
          ctx.timerService().registerProcessingTimeTimer(timer)
          in2Timer.update(timer)
        }
        ListData2.add(in2)

      }


      override def onTimer(timestamp: Long, ctx: CoProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {

        //如果这个定时器是第二条流注册的时间触发的
        if (timestamp == in2Timer.value()) {
          println(TimeUtil.timestampToString(timestamp))
          //清空第二条流的数据和定时器数据
          ListData2.clear()
          in2Timer.clear()
        }
        else {//第一条流延迟计算时间到了
          //取出两个缓存中的数据
          val it1 = ListData1.get.iterator()
          val it2 = ListData2.get.iterator()
          //如果第一条流和第二条流都有数据，则把两条流的数据合并输出
          if (it1.hasNext && it2.hasNext) {
            val result: Iterator[String] = CollectionUtil.twoItLeftJoin(it1, it2)
            while (result.hasNext) {
              out.collect("latecompute>>>>"+result.next())
            }
          }
          else{
            //否则只输出第一条流中的数据
            while (it1.hasNext) {
              out.collect("latecompute no input2>>>>"+it1.next())
            }
          }
          //清空第一条流的数据
          ListData1.clear()
        }

      }
    }


    //连接两条流
    val resultDS = keyed_DStream1
      .connect(keyed_DStream2)
      .process(new MyProcessFunction)


      resultDS.print("resultDS")

    // 启动executor，执行任务
    env.execute()


  }

}
