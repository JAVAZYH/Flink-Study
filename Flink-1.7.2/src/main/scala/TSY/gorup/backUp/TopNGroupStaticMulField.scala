//package TSY.gorup.backUp
//
//import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.util.Collector
//
//import scala.collection.immutable.TreeMap
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: aresyhzhang
//  * \* Date: 2021/2/1
//  * \* Time: 14:31
//  * 对TopN进行统计，指定过期时间，TopN的字段和升序降序规则不固定。
//  * \*/
//object TopNGroupStatic {
//  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val inputDataStream: DataStream[String] = env.socketTextStream("9.134.217.5",9999)
//
//
//
//    //判断数据类型，判断排序方式，判断topN的数量
//
//    //统计结果时长(秒)
//    val timeValid="9999".toLong
//
//    def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000
//
//    def fieldIsNull(inputStr:String): Boolean ={
//      null==inputStr || inputStr=="null" || inputStr==""
//    }
//
//
//    /**
//      * 将列名解析为索引
//      * @param parseStr 待解析字符串
//      * @param inputList 列名集合
//      * @return 索引字符串
//      */
//    def parseColumnsName(parseStr:String,inputList:List[List[String]]):String={
//      //解析列名
//      val inputColumnsArr=ArrayBuffer[String]()
//      for (elem <- inputList) {
//        val columnName: String = elem.head
//        inputColumnsArr.append(columnName)
//      }
//      //解析索引
//      val IndexArr=ArrayBuffer[Int]()
//      for (columnName <- parseStr.split(',')) {
//        //+1是为了兼容旧代码
//        val index: Int = inputColumnsArr.indexOf(columnName)+1
//        if(index==0){
//          return  null
//        }
//        IndexArr.append(index)
//      }
//      IndexArr.mkString(",")
//    }
//
//    object descOrdering extends Ordering[Double]{
//      override def compare(x: Double, y: Double): Int = {
//        if(x>y){
//          -1
//        }
//        else if(x==y){
//          0
//        }
//       else{
//          1
//        }
//      }
//    }
//
//    //主要key，用来进行keyby操作
//    val mainKeyPos="2|3|4".split('|').map(_.toInt-1)
//    val inColumnLists=List(
//      List(List("log_time", "STRING"),List("uin", "STRING"),List("policy_id", "BIGINT"),List("punish_time","BIGINT"))
//      )
//
//    //TopN的字段与对应升降规则，解析字段名为索引
//    val topNFieldArr = "punish_time#desc#10#true|punish_time#asc#10#false"
//      .split('|')
//      .map(str=>{
//        val inputArr: Array[String] = str.split('#')
//        val index: String = parseColumnsName(inputArr.head,inColumnLists.head)
//        inputArr.update(0,index)
//        inputArr.mkString("#")
//      })
//
//
//
//    class TopNKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
//
//      private var timeState: ValueState[Long] = _
//      private var cacheMapState:MapState[String,TreeMap[Double,String]]=_
//
//      override def open(parameters: Configuration): Unit = {
//        timeState = getRuntimeContext.getState(
//          new ValueStateDescriptor[Long]("timeState", classOf[Long])
//        )
//        cacheMapState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[String,TreeMap[Double,String]]("cacheMapState",classOf[String],classOf[TreeMap[Double,String]])
//        )
//
//      }
//
//      override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
//
//        val inputArr: Array[String] = value.split('|')
//        //注册定时器
//        if (timeState.value() == 0L){
//          var timer=0L
//          val ts=ctx.timerService().currentProcessingTime()
//          if (timeValid == 9999L) {
//            val tomorrowTS = tomorrowZeroTimestampMs(ts, 8)
//            ctx.timerService().registerProcessingTimeTimer(tomorrowTS)
//            timer = tomorrowTS
//          }
//          else {
//            timer = ts + (timeValid * 1000)
//            ctx.timerService().registerProcessingTimeTimer(timer)
//          }
//          timeState.update(timer)
//        }
//        //缓存多字段TopN
//        for (elem <- topNFieldArr) {
//
//          //参数分解
//          val fieldIndex = elem.split('#').head.toInt-1
//          val sortType=elem.split('#')(1)
//          val topNum: Int = elem.split('#')(2).toInt
//          val isDistinct=elem.split('#').last.toBoolean
//          val fieldValue=inputArr(fieldIndex).toDouble
//
//          //同一个字段可能会有升降序
//           var cacheTreeMap: TreeMap[Double, String] = cacheMapState.get(fieldIndex.toString+"_"+sortType)
//
//          //构建treeMap
//          if(cacheTreeMap==null){
//            if (sortType=="desc"){
//              cacheTreeMap=new TreeMap[Double,String]()(descOrdering)
//            }else{
//              cacheTreeMap=new TreeMap[Double,String]()
//            }
//          }
//          val cacheList: List[Double] = cacheTreeMap.keys.toList
//
//          cacheTreeMap+=(fieldValue->value)
//
//          if(cacheTreeMap.size>topNum){
//            cacheTreeMap=cacheTreeMap.dropRight(1)
//          }
//          cacheMapState.put(fieldIndex.toString+"_"+sortType,cacheTreeMap)
//
//          val updateList: List[Double] = cacheTreeMap.keys.toList
//
//
//
//          //每次一旦topn有更新，就需要把更新后的排名都输出一遍到下游es
//          //判断list是否有改变
//          if(!updateList.equals(cacheList)){
//            //获取改变及之后的数据并且输出
//            val diffElem: Double = updateList.diff(cacheList).head
//            val diffElemIndex: Int = updateList.indexOf(diffElem)
//            //获取该元素之后的所有数据，更新排行榜
//            val updateKeyList: List[Double] = updateList.takeRight(updateList.length-diffElemIndex)
//            for (key <- updateKeyList) {
//              out.collect(cacheTreeMap.getOrElse(key,null)+"|"+updateKeyList.indexOf(key)+1)
//            }
//          }
//
//        }
//
//      }
//
//      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
//        timeState.clear()
//        cacheMapState.clear()
//      }
//    }
//
//    inputDataStream.keyBy(str => {
//      val arr: Array[String] = str.split('|')
//      var key = ""
//      for (ele <- mainKeyPos) {
//        key += '_'+arr(ele)
//      }
//      key
//    })
//      .process(new TopNKeyedProcessFunction)
//        .print()
//
//    env.execute()
//
//  }
//
//}