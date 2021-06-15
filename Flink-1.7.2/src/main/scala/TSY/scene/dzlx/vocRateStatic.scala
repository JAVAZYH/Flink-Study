//package TSY.scene.dzlx
//
//
//import java.util
//
//import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction
//import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
//import org.apache.flink.types.Row
//import org.apache.flink.util.Collector
//import org.slf4j.{Logger, LoggerFactory}
//import org.apache.flink.api.scala._
//
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//import scala.util.Random
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: aresyhzhang
//  * \* Date: 2020/11/24
//  * \* Time: 15:15
//  * \*/
//
//
//object vocRateStatic {
//  def main(args: Array[String]): Unit = {
//
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    val LOG: Logger = LoggerFactory.getLogger(this.getClass)
//
//    val punishiDS = env.socketTextStream("9.134.217.5",9999)
//
//    val vocDS = env.socketTextStream("9.134.217.5",9998)
//
//
//     val punishiGameIdPos=2-1
//     val punishiOpenIdPos=3-1
//     val punishiSrcPos=5-1
//     val punishiPolicyIdPos=4-1
//     val punishiTimestampPos=1-1
//     val punishiUinPos=6-1
//
//     val vocGameIdPos=1-1
//     val vocUinPos=2-1
//     val vocTimeStampPos=3-1
//
//
//    val punishiKeyedStream: KeyedStream[String, String] = punishiDS.filter(str => {
//      val inputArr: Array[String] = str.split('|')
//      val gameId = inputArr(2 - 1)
//      val openId = inputArr(3 - 1)
//      val punishSrc = inputArr(5 - 1)
//      val policyId = inputArr(4 - 1)
//      val timestamp = inputArr(1 - 1)
//      null != inputArr(punishiGameIdPos) && null != openId && null != punishSrc && null != policyId && null != timestamp
//    })
//      .map(str=>{
//        val inputArr: Array[String] = str.split('|')
//        val gameId=inputArr(2-1)
//        gameId+"-"+Random.nextInt(100)+"#"+inputArr.mkString("|")
//       })
//      .keyBy(_.split('#').head)
//
//
//    val vocKeyedStream: KeyedStream[String, String] = vocDS.filter(str => {
//      val inputArr: Array[String] = str.split('|')
//      val gameId = inputArr(1 - 1)
//      val openId = inputArr(2 - 1)
//      val timestamp = inputArr(3 - 1)
//      null != gameId && null != openId && null != timestamp
//    })
//      .map(str=>{
//        val inputArr: Array[String] = str.split('|')
//        val gameId=inputArr(1-1)
//        gameId+"-"+Random.nextInt(100)+"#"+inputArr.mkString("|")
//      })
//      .keyBy(_.split('#').head)
//
//
//    punishiKeyedStream
//      .connect(vocKeyedStream)
//      .process(new countsProcessFunction)
//      .uid("allGame-vocRate")
//
//
//    class countsProcessFunction extends CoProcessFunction[String,String,String]{
//
//      private var punishiVocState:MapState[(String,String),(mutable.Set[String],mutable.Set[String])]=_
//      private var uinPolicySrcState:MapState[String,ArrayBuffer[String]]=_
//      private var punishiUinState:ValueState[mutable.Set[String]]=_
//      private var timeFieldMapState:MapState[Long,ArrayBuffer[String]]=_
//
//      override def open(parameters: Configuration): Unit = {
//        punishiVocState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[(String,String),(mutable.Set[String],mutable.Set[String])]("punishiVocState", classOf[(String, String)], classOf[(mutable.Set[String], mutable.Set[String])]))
//        uinPolicySrcState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[String,ArrayBuffer[String]]("uinPolicySrcState",classOf[String],classOf[ArrayBuffer[String]])
//        )
//        punishiUinState=getRuntimeContext.getState(
//          new ValueStateDescriptor[mutable.Set[String]]("punishiUinState",classOf[mutable.Set[String]])
//        )
//        timeFieldMapState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[Long,ArrayBuffer[String]]("timeFieldMapState",classOf[Long],classOf[ArrayBuffer[String]])
//        )
//      }
//
//      override def processElement1(value: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
//        try {
//        val inputArr: Array[String] = value.split('|')
//        val punishiPolicyId=inputArr(punishiPolicyIdPos)
//        val punishiSrc=inputArr(punishiSrcPos)
//        val punishiUin: String = inputArr(punishiUinPos)
//
////        LOG.info(s"""punishi处理processElement1的处理时间为：${context.timerService().currentProcessingTime()}""")
//        //缓存单条处罚数据
//        val ts: Long =ctx.timerService().currentProcessingTime()+10000L
//        ctx.timerService().registerProcessingTimeTimer(ts)
//        val cacheArr: ArrayBuffer[String] = if(timeFieldMapState.get(ts)==null)ArrayBuffer[String]()else timeFieldMapState.get(ts)
//        cacheArr.append(s"""${punishiPolicyId}_${punishiSrc}_${punishiUin}#punshi""")
//        timeFieldMapState.put(ts,cacheArr)
//
//        val punishiVocTuple: (mutable.Set[String], mutable.Set[String]) =punishiVocState.get((punishiPolicyId,punishiSrc))
//        if(punishiVocTuple==null)punishiVocTuple==Tuple2(mutable.Set(""),mutable.Set(""))
//
//        //更新策略号处罚源命中的uin
//        val punishiUinSet: mutable.Set[String] = punishiVocTuple._1
//        val vocUinSet: mutable.Set[String] = punishiVocTuple._2
//        punishiUinSet.add(punishiUin)
//        punishiVocState.put((punishiPolicyId,punishiSrc),(punishiUinSet,vocUinSet))
//
//        //更新处罚源命中的uin
//        val uinSet: mutable.Set[String] = punishiUinState.value()
//        uinSet.add(punishiUin)
//        punishiUinState.update(uinSet)
//
//        //更新uin命中的策略号和处罚源
//        val uinPolicySrcArr: ArrayBuffer[String] = uinPolicySrcState.get(punishiUin)
//        uinPolicySrcArr.append(punishiPolicyId+"#"+punishiSrc)
//        uinPolicySrcState.put(punishiUin,uinPolicySrcArr)
//
//        out.collect(value+"|"+punishiUinSet.size+"|"+vocUinSet.size)
//
//        }catch {
//          case exception: Exception=>{
//            LOG.error("处理处罚数据时出错，出错的数据为:"+value)
//            LOG.error(exception.getMessage)
//          }
//        }
//      }
//
//      override def processElement2(value: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
//
////        LOG.info(s"""voc处理processElement2的处理时间为：${context.timerService().currentProcessingTime()}""")
//        val inputArr: Array[String] = value.split('|')
//        val vocUin: String = inputArr(vocUinPos)
//        val punishiUinSet: mutable.Set[String] = punishiUinState.value()
//
//        if (punishiUinSet.contains(vocUin)){
//          //找到该uin处于哪个策略号中
//          val policySrcArr: ArrayBuffer[String] = uinPolicySrcState.get(vocUin)
//          for (elem <- policySrcArr) {
//            val policy = elem.split('#').head
//            val punishiSrc = elem.split('#').last
//
//            val ts: Long =ctx.timerService().currentProcessingTime()+86400000L
//            ctx.timerService().registerProcessingTimeTimer(ts)
//            val cacheArr: ArrayBuffer[String] = if(timeFieldMapState.get(ts)==null)ArrayBuffer[String]()else timeFieldMapState.get(ts)
//            cacheArr.append(s"""${policy}_${punishiSrc}_${vocUin}#voc""")
//            timeFieldMapState.put(ts,cacheArr)
//
//
//
//          }
//        }
//
//        val keysIt: util.Iterator[(String, String, String)] = punishVocState.keys().iterator()
//        try {
//          while (keysIt.hasNext) {
//            val keyTuple: (String, String, String) = keysIt.next()
//            if (voc.gameId == keyTuple._1) {
//              val punishUinSet: mutable.Set[String] = punishVocState.get(keyTuple)._1
//              if (punishUinSet.contains(voc.openId) && openidMap.contains(voc.openId)) {
//
//                val ts: Long =context.timerService().currentProcessingTime()+86400000L
//                context.timerService().registerProcessingTimeTimer(ts)
//                val cacheArr: ArrayBuffer[String] = if(timeFieldMapState.get(ts)==null)ArrayBuffer[String]()else timeFieldMapState.get(ts)
//                cacheArr.append(s"""${keyTuple._1}_${keyTuple._2}_${keyTuple._3}#voc#${voc.openId}""")
//                timeFieldMapState.put(ts,cacheArr)
//                //              timeFieldMapState.put(ts,s"""${keyTuple._1}_${keyTuple._2}_${keyTuple._3}#voc#${voc.openId}""")
//
//                val vocTuple: (mutable.Set[String], mutable.Set[String]) = punishVocState.get(keyTuple)
//                vocTuple._2.add(voc.openId)
//                punishVocState.put(keyTuple, (vocTuple._1, vocTuple._2))
//                var count=1
//                if(vocTuple._1.size>0){
//                  count = vocTuple._1.size
//                }
//                val rate:Double = (vocTuple._2.size - 1)*1.0/count
//                val result: VocMatchResult = VocMatchResult(voc.timestamp, keyTuple._1, keyTuple._3, keyTuple._2, openidMap.get(voc.openId), count, vocTuple._2.size - 1, rate, "voc")
//                collector.collect(result)
//              }
//            }
//          }
//        }
//        catch {
//          case exception: Exception=>{
//            LOG.error("处理voc数据时出错，出错的数据为:"+voc.toString)
//            LOG.error(exception.getMessage)
//            println(exception.getMessage)
//            throw exception
//          }
//        }
//
//      }
//
//    }
//
//
//
//
//
//
//
//    class vocRateProcessFunction extends CoProcessFunction[PunishModel, VocModel, VocMatchResult] {
//
//      /**
//        * punishVocState
//        * openidMap
//        */
//      private var punishVocState: MapState[(String, String, String), (mutable.Set[String], mutable.Set[String])]=_
//      private var openidMap: MapState[String, String]=_
//      private var timeFieldMapState:MapState[Long,ArrayBuffer[String]]=_
//
//
//      override def open(parameters: Configuration): Unit = {
//        punishVocState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[(String, String, String), (mutable.Set[String], mutable.Set[String])]("map-state", classOf[(String, String, String)], classOf[(mutable.Set[String], mutable.Set[String])]))
//        openidMap= getRuntimeContext.getMapState(
//          new MapStateDescriptor[String, String]("qq-openid", classOf[String], classOf[String]))
//        timeFieldMapState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[Long,ArrayBuffer[String]]("timeFieldMapState",classOf[Long],classOf[ArrayBuffer[String]])
//        )
//      }
//
//      override def processElement1(punish: PunishModel, context: CoProcessFunction[PunishModel, VocModel, VocMatchResult]#Context, collector: Collector[VocMatchResult]): Unit = {
//        val punishTuple: (mutable.Set[String], mutable.Set[String]) = punishVocState.get((punish.gameId, punish.policyId, punish.punishSrc))
//        LOG.info(s"""punishi处理processElement1的处理时间为：${context.timerService().currentProcessingTime()}""")
//        val ts: Long =context.timerService().currentProcessingTime()+10000L
//        context.timerService().registerProcessingTimeTimer(ts)
//        val cacheArr: ArrayBuffer[String] = if(timeFieldMapState.get(ts)==null)ArrayBuffer[String]()else timeFieldMapState.get(ts)
//        cacheArr.append(s"""${punish.gameId}_${punish.policyId}_${punish.punishSrc}#punshi#${punish.openId}""")
//        timeFieldMapState.put(ts,cacheArr)
//        //        timeFieldMapState.put(ts,s"""${punish.gameId}_${punish.policyId}_${punish.punishSrc}#punshi#${punish.openId}""")
//
//        try {
//          if (punishTuple == null) {
//            punishVocState.put((punish.gameId, punish.policyId, punish.punishSrc), (mutable.Set(punish.openId), mutable.Set("")))
//          } else {
//            punishTuple._1.add(punish.openId)
//            punishVocState.put((punish.gameId, punish.policyId, punish.punishSrc), (punishTuple._1, punishTuple._2))
//          }
//          openidMap.put(punish.openId, punish.uin)
//          val punishTupleNew: (mutable.Set[String], mutable.Set[String]) = punishVocState.get((punish.gameId, punish.policyId, punish.punishSrc))
//          var count = 1L
//          if (punishTupleNew._1.size > 0) {
//            count = punishTupleNew._1.size
//          }
//          val rate: Double = (punishTupleNew._2.size - 1) * 1.0 / count
//          val result: VocMatchResult = VocMatchResult(punish.timestamp, punish.gameId, punish.punishSrc, punish.policyId, punish.uin, count, punishTupleNew._2.size - 1, rate, "punish")
//          collector.collect(result)
//        }catch {
//          case exception: Exception=>{
//            LOG.error("处理处罚数据时出错，出错的数据为:"+punish.toString)
//            LOG.error(exception.getMessage)
//            throw exception
//          }
//        }
//      }
//
//      override def processElement2(voc: VocModel, context: CoProcessFunction[PunishModel, VocModel, VocMatchResult]#Context, collector: Collector[VocMatchResult]): Unit = {
//        //gameIdPolicyIdPunishiSrc
//        LOG.info(s"""voc处理processElement2的处理时间为：${context.timerService().currentProcessingTime()}""")
//        val keysIt: util.Iterator[(String, String, String)] = punishVocState.keys().iterator()
//        try {
//          while (keysIt.hasNext) {
//            val keyTuple: (String, String, String) = keysIt.next()
//            if (voc.gameId == keyTuple._1) {
//              val punishUinSet: mutable.Set[String] = punishVocState.get(keyTuple)._1
//              if (punishUinSet.contains(voc.openId) && openidMap.contains(voc.openId)) {
//
//                val ts: Long =context.timerService().currentProcessingTime()+86400000L
//                context.timerService().registerProcessingTimeTimer(ts)
//                val cacheArr: ArrayBuffer[String] = if(timeFieldMapState.get(ts)==null)ArrayBuffer[String]()else timeFieldMapState.get(ts)
//                cacheArr.append(s"""${keyTuple._1}_${keyTuple._2}_${keyTuple._3}#voc#${voc.openId}""")
//                timeFieldMapState.put(ts,cacheArr)
//                //              timeFieldMapState.put(ts,s"""${keyTuple._1}_${keyTuple._2}_${keyTuple._3}#voc#${voc.openId}""")
//
//                val vocTuple: (mutable.Set[String], mutable.Set[String]) = punishVocState.get(keyTuple)
//                vocTuple._2.add(voc.openId)
//                punishVocState.put(keyTuple, (vocTuple._1, vocTuple._2))
//                var count=1
//                if(vocTuple._1.size>0){
//                  count = vocTuple._1.size
//                }
//                val rate:Double = (vocTuple._2.size - 1)*1.0/count
//                val result: VocMatchResult = VocMatchResult(voc.timestamp, keyTuple._1, keyTuple._3, keyTuple._2, openidMap.get(voc.openId), count, vocTuple._2.size - 1, rate, "voc")
//                collector.collect(result)
//              }
//            }
//          }
//        }
//        catch {
//          case exception: Exception=>{
//            LOG.error("处理voc数据时出错，出错的数据为:"+voc.toString)
//            LOG.error(exception.getMessage)
//            println(exception.getMessage)
//            throw exception
//          }
//        }
//      }
//
//      override def onTimer(timestamp: Long, ctx: CoProcessFunction[PunishModel, VocModel, VocMatchResult]#OnTimerContext, out: Collector[VocMatchResult]): Unit = {
//        val cacheArr: ArrayBuffer[String] = timeFieldMapState.get(timestamp)
//        for (key <- cacheArr) {
//          //更新处罚voc的状态
//          val keyArr = key.split('#').head.split('_')
//          val keyTuple=Tuple3(keyArr(0),keyArr(1),keyArr(2))
//          val keyType=key.split('#')(1)
//          val uin: String = key.split('#').last
//          val uinTuple: (mutable.Set[String], mutable.Set[String]) = punishVocState.get(keyTuple)
//          keyType match{
//            case "punishi"=>{
//              val punishUinSet: mutable.Set[String] = uinTuple._1
//              punishUinSet.remove(uin)
//              punishVocState.put(keyTuple,(punishUinSet,uinTuple._2))
//            }
//            case "voc"=>{
//              val vocUinSet: mutable.Set[String] = uinTuple._2
//              vocUinSet.remove(uin)
//              punishVocState.put(keyTuple,(uinTuple._1,vocUinSet))
//            }
//          }
//        }
//
//        //          val key = timeFieldMapState.get(timestamp)
//        //
//        //          //更新处罚voc的状态
//        //          val keyArr = key.split('#').head.split('_')
//        //          val keyTuple=Tuple3(keyArr(0),keyArr(1),keyArr(2))
//        //          val keyType=key.split('#')(1)
//        //          val uin: String = key.split('#').last
//        //          val uinTuple: (mutable.Set[String], mutable.Set[String]) = punishVocState.get(keyTuple)
//        //          keyType match{
//        //            case "punishi"=>{
//        //              val punishUinSet: mutable.Set[String] = uinTuple._1
//        //              punishUinSet.remove(uin)
//        //              punishVocState.put(keyTuple,(punishUinSet,uinTuple._2))
//        //            }
//        //            case "voc"=>{
//        //              val vocUinSet: mutable.Set[String] = uinTuple._2
//        //              vocUinSet.remove(uin)
//        //              punishVocState.put(keyTuple,(uinTuple._1,vocUinSet))
//        //            }
//        //          }
//
//
//
//
//
//      }
//    }
//
//
//    val result: DataStream[VocMatchResult] = punishStream.connect(vocStream)
//      .process(new vocRateProcessFunction)
//      .uid("allGame-vocRate-77499-118")
//
//    val resultRow = new Row(9)
//    val resultDataStream: DataStream[Row] = result
//      .map(x => {
//        resultRow.setField(0, x.eventTime)
//        resultRow.setField(1, x.gameId)
//        resultRow.setField(2, x.punishSrc)
//        resultRow.setField(3, x.policyId)
//        resultRow.setField(4, x.openId)
//        resultRow.setField(5, x.punishCount)
//        resultRow.setField(6, x.vocCount)
//        resultRow.setField(7, x.rate)
//        resultRow.setField(8, x.dataType)
//        resultRow
//      })
//    resultDataStream.print()
//    env.execute()
//  }
//
//
//}