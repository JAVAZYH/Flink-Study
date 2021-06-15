//package TSY.gorup.backUp;
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: aresyhzhang
//  * \* Date: 2020/10/21
//  * \* Time: 9:31
//  * \*/
//object CountFirstStatic {
//  def main(args: Array[String]): Unit = {
//
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//
//    //统计类型
//    val static_type="0"
//    //是否去重
//    val is_distinct="true"
//    //联合主键位置
//    val main_key_pos="2"
//    //统计字段位置
//    val field_pos="3".toInt-1
//    //时间字段位置
//    val time_pos="1".toInt-1
//    //统计结果时长(秒)
//    val time_valid="5".toLong
//    //时间字段类型
//    val time_type="yyyy-MM-dd HH:mm:ss"
//
//
//
//    def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
//      val timeFormat = new SimpleDateFormat(format)
//      val timestamp = timeFormat.parse(timeStr).getTime
//      timestamp
//    }
//    //如果过期时间选的是今天
//    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000
//
//    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(3)) {
//        override def extractTimestamp(element: String): Long = {
//          val time: String = element.split('|')(time_pos)
//          stringToTimestamp(time)
//        }
//      })
//
//
//    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
//      private var countSetState:ValueState[mutable.Set[String]]=_
//      private var countLongState:ValueState[Long]=_
//      private var contentState:ValueState[mutable.ArrayBuffer[String]]=_
//      private var timeState:ValueState[Long]=_
//      //把谁注册了什么内容记录下来
//      private var timeFieldMapState:MapState[String,String]=_
//
//      override def open(parameters: Configuration): Unit = {
//        countSetState=getRuntimeContext.getState(
//          new ValueStateDescriptor[mutable.Set[String]]("countSetState", classOf[mutable.Set[String]])
//        )
//        countLongState=getRuntimeContext.getState(
//          new ValueStateDescriptor[Long]("countLongState", classOf[Long])
//        )
//        contentState=getRuntimeContext.getState(
//          new ValueStateDescriptor[mutable.ArrayBuffer[String]]("contentState", classOf[mutable.ArrayBuffer[String]])
//        )
//        timeState=getRuntimeContext.getState(
//          new ValueStateDescriptor[Long]("timeState", classOf[Long])
//        )
//        timeFieldMapState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[String,String]("timeFieldMapState",classOf[String],classOf[String])
//        )
//      }
//
//      override def processElement(input: String,
//                                  ctx: KeyedProcessFunction[String, String, String]#Context,
//                                  out: Collector[String]): Unit = {
//        val input_arr: Array[String] = input.split('|')
//        val field=input_arr(field_pos)
//        val time=input_arr(time_pos)
//
//        val ts: Long = time_type match {
//          case "timeStamp" => time.toLong
//          case "yyyy-MM-dd HH:mm:ss" => stringToTimestamp(time)
//        }
//        //如果选用当天时间
//        if (time_valid==9999L){
//          if(timeState.value()==0L) {
//            val tomorrow_timer: Long = tomorrowZeroTimestampMs(ts,8)
//            ctx.timerService().registerEventTimeTimer(tomorrow_timer)
//            timeState.update(tomorrow_timer)
//          }
//        }
//        else{
//          val timer=ts+(time_valid*1000)
//          ctx.timerService().registerEventTimeTimer(timer)
//          //把这条数据记录下来，并且在key中附带上时间戳
//          val key=ctx.getCurrentKey+"_"+timer.toString
//          timeFieldMapState.put(key,field)
//          timeState.update(timer)
//
//          //把比之前时间戳小的状态全部干掉
//        }
//
//
//
////        var count=0L
////        var content=""
//
//        //这里的set不再是由状态中获取而来，而是每次来一条数据都会把状态数据放到set集合中去重
//        import scala.collection.JavaConversions._
//        import  scala.collection.JavaConverters._
//        val fieldSet: Set[String] = timeFieldMapState.values().iterator().toSet
//        val count: Int = fieldSet.size
//
//        out.collect(input+"|"+count)
//
////        //更新去重的量级/内容状态
////        def updateCountSetState() ={
////          val fieldSet: mutable.Set[String] = if(countSetState.value()==null)mutable.Set[String]()else countSetState.value()
////          fieldSet.add(field)
////          countSetState.update(fieldSet)
////          fieldSet
////        }
//
////        //更新不去重的量级状态
////        def updateCountLongState() ={
////          var tmp: Long = if(countLongState.value()==null) 0 else countLongState.value()
////          tmp+=1
////          countLongState.update(tmp)
////          tmp
////        }
////
////        //更新不去重的内容状态
////        def updateContentState() ={
////          val arr: ArrayBuffer[String] = if(contentState.value()==null) ArrayBuffer[String]()else contentState.value()
////          arr.append(field)
////          contentState.update(arr)
////          arr
////        }
//
////        //统计类型
////        static_type match{
////          //量级统计
////          case "0"=>{
////            is_distinct match {
////              case "true"=>{
////                count = updateCountSetState().size.toLong
////              }
////              case "false"=>{
////                count=updateCountLongState()
////              }
////            }
////            out.collect(input+"|"+count)
////          }
////          //内容统计
////          case "1"=>{
////            is_distinct match {
////              case "true"=>{//进行去重统计
////                content=updateCountSetState().mkString(",")
////              }
////              case "false"=>{//不做去重统计
////                content=updateContentState().mkString(",")
////              }
////            }
////            out.collect(input+"|"+content)
////          }
////          //同时统计
////          case "2"=>{
////            is_distinct match {
////              case "true"=>{
////                content=updateCountSetState().mkString(",")
////                count = updateCountSetState().size.toLong
////              }
////              case "false"=>{
////                content=updateContentState().mkString(",")
////                count=updateCountLongState()
////              }
////            }
////            out.collect(input+"|"+count+"|"+content)
////          }
////        }
//
//
//      }
//
//      override def onTimer(timestamp: Long,
//                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
//                           out: Collector[String]): Unit = {
//        def timestampToString(timestamp: Long, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
//          val timeFormat = new SimpleDateFormat(format)
//          val timeStr = timeFormat.format(new Date(timestamp))
//          timeStr
//        }
//        //注册的定时器到了，这个时候需要做的是更新其中的状态也就是把注册的那条数据删除
//        if(timeFieldMapState.keys()!=null){
//          val key=ctx.getCurrentKey+"_"+timestamp
//          timeFieldMapState.remove(key)
////          val field: String = timeFieldMapState.get(key)
//
//          println("当前timeFieldMapState状态中的值是：>>>>>")
//          val it=timeFieldMapState.entries().iterator()
//          while(it.hasNext){
//           val next=it.next()
//            println( next.getKey)
//            println( next.getValue)
//          }
//          println("遍历结束》》》》")
//
////          //把这个field删除
////          val tmpSet: mutable.Set[String] = countSetState.value()
////          tmpSet.remove(field)
////          countSetState.update(tmpSet)
////          println(tmpSet)
//
//          out.collect(
//            s"""定时器触发了，本次清除的keytime是${ctx.getCurrentKey+"_"+timestampToString(timestamp)},
//               |本次清除的key${ctx.getCurrentKey},本次清除的timestamp是${timestampToString(timestamp)},本次清除的field是""".stripMargin)
//        }
//
//        //        countLongState.clear()
////                countSetState.clear()
//        //        contentState.clear()
//        //        timeState.clear()
//      }
//    }
//
//    val resultDS: DataStream[String] = input_DStream1
//      .keyBy(str => {
//        val arr: Array[String] = str.split('|')
//        val mainArr: Array[String] = main_key_pos.split('|')
//        var key = ""
//        for (ele <- mainArr) {
//          key += arr(ele.toInt - 1)
//        }
//        key
//      })
//      .process(new CountStaticKeyedProcessFunction)
//
//    resultDS.print()
//    env.execute()
//  }
//
//
//}