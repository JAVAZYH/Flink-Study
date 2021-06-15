//package DataStreamAPI
//
//import java.text.SimpleDateFormat
//import java.util
//
//import MyUtil.CollectionUtil
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
//      * timeValid1 第一条流先到，等待的时间
//      * timeValid2 第二条流先到，等待的时间
//      */
//    val  timeValid1=3 * 1000L
//    val  timeValid2=3 * 1000L
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
//    env.setParallelism(1)
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
////    //创建两个侧输出流用来存储没有关联到数据的情况
//    val noin1OutputTag = new OutputTag[String]("noin1OutputTag")
////    val noin2OutputTag = new OutputTag[String]("noin2OutputTag")
//
//
// //连接两条流
//    val resultDS: scala.DataStream[String] = keyed_DStream1.connect(keyed_DStream2).process(new CoProcessFunction[String, String, String] {
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
//        val in2 = ListData2.get.iterator()
//        //如果第二个流的数据为空,注册定时器等待第二个元素
//        if (!in2.hasNext ) {
//          val timer = parseTime(in1.split(',')(timePos1) + timeValid1)
//          ctx.timerService().registerEventTimeTimer(timer)
//
//          //把第一个元素缓存，第一个流的定时器也缓存
//          ListData1.add(in1)
//          ListTimer1.add(timer)
//
//        } else {
//          //先把数据缓存方便后续关联
//          ListData1.add(in1)
//
//          //如果第二条流的数据到达
//          //将两个数组中缓存的数据成功组合
//          val it1=ListData1.get.iterator()
//          val it2=ListData2.get.iterator()
//          val result: Iterator[String] = CollectionUtil.twoItLeftJoin(it1,it2)
//          while (result.hasNext){
//            out.collect("p1>>>>>>"+result.next())
//          }
//
//          //把第1条数据流之前注册过的定时器都删除
//          val timerIt: util.Iterator[Long] = ListTimer1.get().iterator()
//          while (timerIt.hasNext){
//            val timer=timerIt.next()
//            ctx.timerService().deleteEventTimeTimer(timer)
//          }
//
//          //把第一条流的缓存数据清空,定时器数据清空
//          ListData1.clear()
//          ListTimer1.clear()
//
//        }
//
//      }
//
//      override def processElement2(in2: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
//        val in1=ListData1.get().iterator()
//        //如果第一个流的数据为空
//        if (!in1.hasNext) {
//          //把第二条流的数据暂时缓存到状态中，等待第一条流的数据到达
//          val timer = parseTime(in2.split(',')(timePos2) + timeValid2)
//          //把第二条流的注册到定时器
//          //保存第二条流的数据到list中
//          ListData2.add(in2)
//          ctx.timerService().registerEventTimeTimer(timer)
//          ListTimer2.add(timer)
//
//        }
//        else {
//          ListData2.add(in2)
//
//          //如果第二条流的数据到达
//          //将两个数组中缓存的数据成功组合
//          val it1=ListData1.get.iterator()
//          val it2=ListData2.get.iterator()
//          val result: Iterator[String] = CollectionUtil.twoItLeftJoin(it1,it2)
//          while (result.hasNext){
//            out.collect("p2>>>>>>"+result.next())
//          }
//
//
//          //把第二条数据流之前注册过的定时器都删除
//          val timerIt: util.Iterator[Long] = ListTimer2.get().iterator()
//          while (timerIt.hasNext){
//            val timer=timerIt.next()
//            ctx.timerService().deleteEventTimeTimer(timer)
//          }
//
//          //清空第二条流缓存的数据和定时器数据
//          ListData2.clear()
//          ListTimer2.clear()
//
//      }
//
//      }
//
//
//      override def onTimer(timestamp: Long, ctx: CoProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
//
//
////        println("in1>>>>>>"+inputData1.value())
////        val it: util.Iterator[String] = ListData2.get().iterator()
////        while (it.hasNext){
////          println("in2>>>>>>"+it.next())
////        }
//
//
//
////      val timer1=in1Timer.value()
//
//
//        /**
//          * 定时器触发：
//          * 如果第一条流的数据不为空，说明
//          * 如果第二条流的数据不为空，说明第二条流中的数据来的时候，第一条流中的数据没空。第二条流中的数据没有第二个到。
//          */
//
//        //如果第一条流的状态不为空并且第二条流中的数据为空，表示没有第二条第一条流的数据来触发计算
//        if (inputData1.value() != null && !ListData2.get().iterator().hasNext) {
//          println("time>>>>>>>>>>>"+MyUtil.TimeUtil.timestampToString(timestamp))
//        out.collect("input1 not join input2>>>"+inputData1.value())
//        }
//
//        //如果第一条流的状态不为空，第二条流的数据状态也不为空，需要关联并且都清空数据
//        if (inputData1.value() != null && ListData2.get().iterator().hasNext) {
//          val it1=ListData1.get.iterator()
//          val it2=ListData2.get.iterator()
//          val result: Iterator[String] = CollectionUtil.twoItLeftJoin(it1,it2)
//          while (result.hasNext){
//            out.collect("p2>>>>>>"+result.next())
//          }
//        }
//
//        //如果第二条流的状态不为空，说明第二条流一直没有触发关联
//        if(!ListData2.get().iterator().hasNext){
//          out.collect("input2 not join input>>>"+ListData2.get().iterator().next())
//        }
//        `
//
//
//
//
//        //如果第二条流的状态不为空，表示第一条流的数据还没到
//        val data2=ListData2.get().iterator().hasNext
//        //如果第二条流的数据不为空，说明第一条流的数据还没到
//        if (data2) {
//          out.collect("input2 not join input>>>"+ListData2.get().iterator().next())
//          //把数据放入到侧输出流中
////          ctx.output(noin1OutputTag,inputData2.value())
//        }
//
//        //        //如果第二条流的状态不为空，表示第一条流的数据还没到
//        //      if(inputData2.value()!=null){
//        //        ctx.output(noin1OutputTag,inputData2.value())
//        //      }
//        //        //如果第一条流的状态不为空，表示第二条流的数据还没到
//        //       if (inputData1.value()!=null){
//        //         ctx.output(noin2OutputTag,inputData1.value())
//        //       }
//        //        inputData1.clear()
//        //        inputData2.clear()
//
//        /**
//          * 定时器触发，意味着用户设置的时间范围到了，前面已经做了处理，
//          * 这里需要把所有状态都清空
//          */
//        inputData1.clear()
//        in1Timer.clear()
//
//        ListData2.clear()
//        ListTimer2.clear()
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
