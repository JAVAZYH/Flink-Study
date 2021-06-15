package datastream_api.common



import MyUtil.TimeUtil
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * 处理数据倾斜
  */

object KeyByDataSkew {

  class SalesData extends SourceFunction[String]{
    private val random=new Random()
    val nameList=List[String]("衣服","裤子","鞋")

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      ctx.collect(nameList(random.nextInt(3))
      +","+
      random.nextInt(20)
      +","+
        TimeUtil.timestampToString(System.currentTimeMillis())
      )

    }

    override def cancel(): Unit = ???
  }


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(2000)
    val stream: DataStream[String] = env.socketTextStream("localhost", 9999)
    val typeAndData: DataStream[(String, Long)] = stream.map(x => (x.split(",")(0), x.split(",")(1).toLong))
    val dataStream: DataStream[(String, Long)] = typeAndData
      .map(x => (x._1 + "-" + scala.util.Random.nextInt(100), x._2))
    val keyByAgg: DataStream[DataJast] = dataStream.keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .aggregate(new CountAggregate())
    keyByAgg.print("第一次keyby输出")
    val result: DataStream[DataJast] = keyByAgg.map(data => {
      val newKey: String = data.key.substring(0, data.key.indexOf("-"))
      println(newKey)
      DataJast(newKey, data.count)
    }).keyBy(_.key)
      .process(new MyProcessFunction())
    result.print("第二次keyby输出")


    env.execute()
  }

  case class DataJast(key :String,count:Long)

  //计算keyby后，每个Window中的数据总和
  class CountAggregate extends AggregateFunction[(String, Long),DataJast, DataJast] {

    override def createAccumulator(): DataJast = {
      println("初始化")
      DataJast(null,0)
    }

    override def add(value: (String, Long), accumulator: DataJast): DataJast = {
      if(accumulator.key==null){
        printf("第一次加载,key:%s,value:%d\n",value._1,value._2)
        DataJast(value._1,value._2)
      }else{
        printf("数据累加,key:%s,value:%d\n",value._1,accumulator.count+value._2)
        DataJast(value._1,accumulator.count + value._2)
      }
    }

    override def getResult(accumulator: DataJast): DataJast = {
      println("返回结果："+accumulator)
      accumulator
    }

    override def merge(a: DataJast, b: DataJast): DataJast = {
      DataJast(a.key,a.count+b.count)
    }
  }


  /**
    * 实现：
    *    根据key分类，统计每个key进来的数据量，定期统计数量
    */
  class MyProcessFunction extends  KeyedProcessFunction[String,DataJast,DataJast]{

    val delayTime : Long = 1000L * 30

    lazy val valueState:ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("ccount",classOf[Long]))

    override def processElement(value: DataJast, ctx: KeyedProcessFunction[String, DataJast, DataJast]#Context, out: Collector[DataJast]): Unit = {
      if(valueState.value()==0){
        valueState.update(value.count)
        printf("运行task:%s,第一次初始化数量:%s\n",getRuntimeContext.getIndexOfThisSubtask,value.count)
        val currentTime: Long = ctx.timerService().currentProcessingTime()
        //注册定时器
        ctx.timerService().registerProcessingTimeTimer(currentTime + delayTime)
      }else{
        valueState.update(valueState.value()+value.count)
        printf("运行task:%s,更新统计结果:%s\n" ,getRuntimeContext.getIndexOfThisSubtask,valueState.value())
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, DataJast, DataJast]#OnTimerContext, out: Collector[DataJast]): Unit = {
      //定时器执行，可加入业务操作
      printf("运行task:%s,触发定时器,30秒内数据一共,key:%s,value:%s\n",getRuntimeContext.getIndexOfThisSubtask,ctx.getCurrentKey,valueState.value())

      //定时统计完成，初始化统计数据
      valueState.update(0)
      //注册定时器
      val currentTime: Long = ctx.timerService().currentProcessingTime()
      ctx.timerService().registerProcessingTimeTimer(currentTime + delayTime)
    }
  }



}

