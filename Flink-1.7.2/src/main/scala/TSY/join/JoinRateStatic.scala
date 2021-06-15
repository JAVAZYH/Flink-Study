//package DataStreamAPI
//
//import java.util.Random
//
//import MyUtil.{CollectionUtil, TimeUtil}
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
//import org.apache.flink.api.common.state._
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.flink.util.Collector
//
//class InputSource1  extends SourceFunction[String]{
//  var isRunning: Boolean = true
//  val RandomList1: List[String] = List("111", "222", "333", "444")
//  val RandomList2: List[String] = List("1534280186", "1534280187", "1534280188", "1534280189")
//  val RandomList3: List[String] = List("aaa", "bbb", "ccc", "ddd")
//  val time=TimeUtil.timestampToString(System.currentTimeMillis())
//  private val random = new Random()
//  var number: Long = 0
//
//  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
//    while (isRunning) {
//      ctx.collect(time +"|"
//        +RandomList1(random.nextInt(4))+"|"
//        +RandomList2(random.nextInt(4))+"|"
//        +RandomList3(random.nextInt(4))
//      )
//      number += 1
//      Thread.sleep(500)
//      if(number==100){
//        cancel()
//      }
//    }
//  }
//  override def cancel(): Unit = {
//    isRunning = false
//  }
//}
//
//class InputSource2  extends SourceFunction[String]{
//  var isRunning: Boolean = true
//  val RandomList1: List[String] = List("111", "222", "333", "444")
//  val RandomList2: List[String] = List("1534280186", "1534280187", "1534280188", "1534280189")
//
//
//  val time=TimeUtil.timestampToString(System.currentTimeMillis())
//  private val random = new Random()
//  var number: Long = 0
//
//  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
//    while (isRunning) {
//      ctx.collect(
//           RandomList1(random.nextInt(4))+"|"
//          +RandomList2(random.nextInt(4))+"|"
//          + time
//      )
//      number += 1
//      Thread.sleep(500)
//      if(number==100){
//        cancel()
//      }
//    }
//  }
//  override def cancel(): Unit = {
//    isRunning = false
//  }
//}
//
//object JoinRateStatic {
//   val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//   val input_ds1: DataStream[String] = env.addSource(new InputSource1)
//   val input_ds2: DataStream[String] = env.addSource(new InputSource2)
//
//  /**
//    * mainkeyPos：主键字段的统计偏移，按|进行分割
//    * fieldPos：需要统计的量级字段偏移
//    * isDistinct：量级统计时是否去重
//    * contentPos：内容字段的统计偏移
//    * fieldSplit：内容字段中的分隔符
//    */
//  val mainkeyPos="2|3|4"
//  val mainkeyPos2="1"
//  val contentPos=5-1
//  val fieldPos=6-1
//  val fieldSplit="+"
//  val isDistinct=false
//
//
//   val keyed_DStream1: KeyedStream[String, String] = input_ds1.keyBy(str => {
//    val arr: Array[String] = str.split('|')
//    val mainArr: Array[String] = mainkeyPos.split('|')
//    var key = ""
//    for (ele <- mainArr) {
//      key += arr(ele.toInt - 1)
//    }
//    key
//  })
//
//
//   val keyedDStream2: KeyedStream[String, String] = input_ds2.keyBy(str => {
//    val arr: Array[String] = str.split('|')
//    val mainArr: Array[String] = mainkeyPos2.split('|')
//    var key = ""
//    for (ele <- mainArr) {
//      key += arr(ele.toInt - 1)
//    }
//    key
//  })
//
//  //自定义process函数
//  class MyProcessFunction extends CoProcessFunction[String, String, String] {
//
//
//    /**
//      * ListData1 缓存第一条流数据
//      * ListData2 缓存第二条流数据
//      * in2Timer 缓存第二条流定时器值
//      */
//    private var ListData1: ListState[String] = _
//    private var ListData2: ListState[String] = _
//    private var in2Timer: ValueState[Long] = _
//
//
//    //初始化状态
//    override def open(parameters: Configuration): Unit = {
//      ListData1 = getRuntimeContext.getListState(
//        new ListStateDescriptor[String]("ListData1", classOf[String])
//      )
//      ListData2 = getRuntimeContext.getListState(
//        new ListStateDescriptor[String]("ListData2", classOf[String])
//      )
//      in2Timer = getRuntimeContext.getState(
//        new ValueStateDescriptor[Long]("in1Timer", classOf[Long])
//      )
//
//    }
//
//
//    override def processElement1(in1: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
//      //处理第一条流的元素
//
//
//      if (timeValid1 == 0) {
//        //如果不需要延迟计算，取出第二条流中的缓存数据
//        val it2 = ListData2.get.iterator()
//        if (!it2.hasNext) {
//          //如果第二条流没有数据，则只输出第一条流的数据
//          out.collect(in1)
//        }
//        else {
//          //如果第一条流和第二条流都有数据，则把两条流的数据合并输出
//          while (it2.hasNext) {
//            out.collect(in1+"|"+it2.next())
//          }
//        }
//      }
//
//      //如果需要延迟计算
//      else
//      {
//        //保存第一条流的数据缓存到list中，并且注册定时器
//        ListData1.add(in1)
//        val timer = ctx.timerService().currentProcessingTime() + timeValid1
//        ctx.timerService().registerProcessingTimeTimer(timer)
//      }
//
//    }
//
//    override def processElement2(in2: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
//      /**
//        * 注册一个定时器，在某个时间后清除该缓存，如果之前注册过定时器则不再注册
//        *  第二条流的数据到了，都缓存到list中，
//        */
//      if (in2Timer.value() == 0) {
//        val timer = ctx.timerService().currentProcessingTime() + timeValid2
//        ctx.timerService().registerProcessingTimeTimer(timer)
//        in2Timer.update(timer)
//      }
//      ListData2.add(in2)
//
//    }
//
//
//    override def onTimer(timestamp: Long, ctx: CoProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
//
//      //如果这个定时器是第二条流注册的时间触发的
//      if (timestamp == in2Timer.value()) {
//        println(TimeUtil.timestampToString(timestamp))
//        //清空第二条流的数据和定时器数据
//        ListData2.clear()
//        in2Timer.clear()
//      }
//      else {//第一条流延迟计算时间到了
//      //取出两个缓存中的数据
//      val it1 = ListData1.get.iterator()
//        val it2 = ListData2.get.iterator()
//        //如果第一条流和第二条流都有数据，则把两条流的数据合并输出
//        if (it1.hasNext && it2.hasNext) {
//          val result: Iterator[String] = CollectionUtil.twoItLeftJoin(it1, it2)
//          while (result.hasNext) {
//            out.collect("latecompute>>>>"+result.next())
//          }
//        }
//        else{
//          //否则只输出第一条流中的数据
//          while (it1.hasNext) {
//            out.collect("latecompute no input2>>>>"+it1.next())
//          }
//        }
//        //清空第一条流的数据
//        ListData1.clear()
//      }
//
//    }
//  }
//
//  keyed_DStream1
//    .connect(keyedDStream2)
//    .process(new MyProcessFunction)
//
//
//
//
//
//
//
//}
