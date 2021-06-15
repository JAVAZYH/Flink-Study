//package DataStreamAPI
//
//import java.text.SimpleDateFormat
//import java.util
//
//import MyUtil.{CollectionUtil, TimeUtil}
//import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.{TimeCharacteristic, scala}
//import org.apache.flink.streaming.api.scala.{KeyedStream, OutputTag, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.util.Collector
//
//
//
//
//case class report(
//                   var reporttime:String,
//                   var reportuin:String,
//                   var reporteduin:String
//                 )
//case class dubious(
//
//                    var uin:String,
//                    var dubioustime:String
//                  )
//
//object ConnectJoin {
//  def main(args: Array[String]): Unit = {
//
//    //第一个流和第二个流的位置偏移
//    val  mainKeyPos1=1
//    val  mainKeyPos2=3
//    val timePos1=2-1
//    val timePos2=1-1
//    /**
//      * timeValid1 第一条流的缓存时间
//      * timeValid2 第二条流的等待时间
//      * 单位：秒
//      */
//    val  timeValid1=10 * 1000L
//    val  timeValid2=0 * 1000L
//
//
//    /**
//      *
//      * 1.处理第二条流的数据时，第一条流的数据没到，第二条流缓存到数组
//      * 2.处理第二条流的数据时，第一条流的数据到了，把之前缓存的数据都取出，与第一条流关联，清空数组
//      * 3.处理第一条流的数据时，第二条流的数据到了，把之前缓存的数据都取出，与第一条流关联，清空
//      * 4.处理第一条流的数据时，第二条流的数据没到，把第一条流的数据缓存到数组中
//      */
//
//    // 创建流处理环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
////    env.setParallelism(1)
//
//
//    val input_Dstream1 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\dubious")
//    val input_Dstream2 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\report")
//
//
//    import org.apache.flink.api.scala._
//
//    //可疑账单流
//    val keyed_DStream1: KeyedStream[String, String] = input_Dstream1
//        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
//          override def extractTimestamp(str: String): Long = {
//            val timeStr: String = str.split(',')(timePos1)
//            MyUtil.TimeUtil.stringToTimestamp(timeStr)
//          }
//        })
//      .keyBy(str => str.split(',')(mainKeyPos1 - 1))
//
//    //举报数据流
//    val keyed_DStream2: KeyedStream[String, String] = input_Dstream2
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
//        override def extractTimestamp(str: String): Long = {
//          val timeStr: String = str.split(',')(timePos2)
//          MyUtil.TimeUtil.stringToTimestamp(timeStr)
//        }
//      })
//      .keyBy(str => str.split(',')(mainKeyPos2 - 1))
//
//
//
// //连接两条流
//    val resultDS: scala.DataStream[String] = keyed_DStream1
//      .connect(keyed_DStream2)
//      .process(new CoProcessFunction[String, String, String] {
//
//      private var inputData1: ValueState[String] = _
//      private var inputData2: ValueState[String] = _
//      private var ListData1: ListState[String] = _
//      private var ListData2: ListState[String] = _
//      private var ListTimer1: ListState[Long] = _
//      private var ListTimer2: ListState[Long] = _
//      private var in1Timer: ValueState[Long] = _
//      private var in2Timer: ValueState[Long] = _
//
//      //输入时间字符串，转换为毫秒时间戳
//      def parseTime(timeStr: String) = {
//        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//        val timeStamp: Long = format.parse(timeStr).getTime
//        timeStamp
//      }
//
//      override def open(parameters: Configuration): Unit = {
//        inputData1 = getRuntimeContext.getState(
//          new ValueStateDescriptor[String]("in1", classOf[String]))
//        inputData2 = getRuntimeContext.getState(
//          new ValueStateDescriptor[String]("in2", classOf[String]))
//        ListData1 = getRuntimeContext.getListState(
//          new ListStateDescriptor[String]("ListData1", classOf[String])
//        )
//        ListData2 = getRuntimeContext.getListState(
//          new ListStateDescriptor[String]("ListData2", classOf[String])
//        )
//        ListTimer1 = getRuntimeContext.getListState(
//          new ListStateDescriptor[Long]("ListTimer1", classOf[Long])
//        )
//        ListTimer2 = getRuntimeContext.getListState(
//          new ListStateDescriptor[Long]("ListTimer2", classOf[Long])
//        )
//        in1Timer = getRuntimeContext.getState(
//          new ValueStateDescriptor[Long]("in1Timer", classOf[Long])
//        )
//        in2Timer = getRuntimeContext.getState(
//          new ValueStateDescriptor[Long]("in2Timer", classOf[Long])
//        )
//
//
//      }
//
//
//      override def processElement1(in1: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
//        /**
//          * 第一条流的数据到了，都缓存到list中，
//          * 并且注册一个定时器，在某个时间后清除该缓存
//          */
//          if(in1Timer.value()==0){
//            val timer = parseTime(in1.split(',')(timePos1)) + timeValid1
//            ctx.timerService().registerEventTimeTimer(timer)
//            in1Timer.update(timer)
//          }
//          ListData1.add(in1)
//
//      }
//
//      override def processElement2(in2: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
//        /**
//          * 如果第二条流的数据到达，让数据进行等待，等待到某个时间之后，触发计算
//        */
//
//        if(timeValid2==0){
//
//
//          //如果触发的是第二条流注册的定时器
//          //将两个数组中缓存的数据成功组合
//          val it1=ListData1.get.iterator()
//          if(!it1.hasNext){//如果第一条流没有数据，则只输出第二条流的数据
//              out.collect("no input1>>>>"+in2)
//          }
//          else{//如果第一条流和第二条流都有数据，则把两条流的数据合并输出
//            while (it1.hasNext){
//              out.collect(it1.next()+in2)
//            }
//          }
//        }
//        else{
//          //保存第二条流的数据缓存到list中，并且注册定时器
//          ListData2.add(in2)
//          val timer =parseTime(in2.split(',')(timePos2)) + timeValid2
//          ctx.timerService().registerEventTimeTimer(timer)
//        }
//
//      }
//
//
//      override def onTimer(timestamp: Long, ctx: CoProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
//
//        //如果这个定时器是第一条流注册的时间触发的
//        if (timestamp==in1Timer.value()){
//          println(TimeUtil.timestampToString(timestamp))
//          //清空第一条流的数据定时器
//          ListData1.clear()
//          in1Timer.clear()
//        }
//        else{
//          //如果触发的是第二条流注册的定时器
//          //将两个数组中缓存的数据成功组合
//          val it1=ListData1.get.iterator()
//          val it2=ListData2.get.iterator()
//          if(!it1.hasNext){//如果第一条流没有数据，则只输出第二条流的数据
//            while (it2.hasNext){
//              out.collect("no input1>>>>"+it2.next())
//            }
//          }
//          else if(it1.hasNext&&it2.hasNext){//如果第一条流和第二条流都有数据，则把两条流的数据合并输出
//            val result: Iterator[String] = CollectionUtil.twoItLeftJoin(it1,it2)
//            while (result.hasNext){
//              out.collect(result.next())
//            }
//          }
//          else{
//            println("input2 not has data>>>")
//          }
//          //清空第二条流的数据
//          ListData2.clear()
//        }
//
//
//      }
//
//    })
//resultDS.print("resultDS")
//
//
//
//
//
////    textDstream.print().setParallelism(1)
//
//    // 启动executor，执行任务
//    env.execute()
//
//
//  }
//
//}
