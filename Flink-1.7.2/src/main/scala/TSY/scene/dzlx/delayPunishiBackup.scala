//package com.tencent.allblue.flink.udf_flowid_nodeid
//
//// AllBlue编译时会自动替换package
//import java.text.SimpleDateFormat
//import java.util
//import java.util.Date
//
//import com.tencent.allblue.flink.task.{TaskRunner, TaskRunnerContext}
//import org.apache.flink.api.common.state._
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.scala.DataStream
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.types.Row
//import org.apache.flink.util.Collector
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//import org.slf4j.LoggerFactory
//import scala.collection.JavaConversions._
//
//class TaskUserCodeRunner extends TaskRunner {
//
//  val LOG = LoggerFactory.getLogger(this.getClass)
//  override def run(dataStreams: List[DataStream[Row]], context: TaskRunnerContext): DataStream[Row] = {
//    // 获取输出数据的RowType
//    implicit val rowType: TypeInformation[Row] = context.getOutRowTypes()
//
//
//    val is_distinct=if(context.params.get("IS_DISTINCT")=="1")true else false
//    //联合主键位置
//    val main_key_pos=context.params.get("MAIN_KEY_POS")
//    //统计字段位置
//    val field_pos=context.params.get("FIELD_POS").toInt-1
//
//    val timeValidPos=context.params.get("TIME_VALID_POS").toInt-1
//
//
//
//    def transRowStreamToStringStream(rowStream: DataStream[Row]): DataStream[String] = {
//      val stringStream = rowStream.map(rows => {
//        var string = ""
//        for (pos <- 0 until rows.getArity) {
//          if (pos == 0)
//            string += rows.getField(pos)
//          else
//            string += "|" + rows.getField(pos)
//        }
//        string
//      })
//      stringStream
//    }
//    //将string流转换为单个row流
//    def transStringStreamToOneRowStream(StringStream: DataStream[String]): DataStream[Row] = {
//      StringStream.map(str=>{
//        val rowResult=new Row(1)
//        rowResult.setField(0,str)
//        rowResult
//      })
//    }
//    val input_DStream1: DataStream[String] =  transRowStreamToStringStream(dataStreams.head)
//
//
//
//
//    class CountStaticKeyedProcessFunction extends KeyedProcessFunction[String,String,String] {
//      private var timeFieldMapState:MapState[String,String]=_
//      private var timeFieldListState:ListState[String]=_
//
//      override def open(parameters: Configuration): Unit = {
//        timeFieldMapState=getRuntimeContext.getMapState(
//          new MapStateDescriptor[String,String]("timeFieldMapState",classOf[String],classOf[String])
//        )
//        timeFieldListState=getRuntimeContext.getListState(
//          new ListStateDescriptor[String]("timeFieldListState",classOf[String])
//        )
//      }
//
//      override def processElement(input: String,
//                                  ctx: KeyedProcessFunction[String, String, String]#Context,
//                                  out: Collector[String]): Unit = {
//        val input_arr: Array[String] = input.split('|')
//        val field=input_arr(field_pos)
//        val time_valid=input_arr(timeValidPos).toLong
//        val timer=ctx.timerService().currentProcessingTime()+(time_valid*1000)
//        ctx.timerService().registerProcessingTimeTimer(timer)
//        val key=ctx.getCurrentKey+"_"+timer.toString
//        var counts=0
//        if(is_distinct){
//          timeFieldMapState.put(key,field)
//          val fieldSet: Set[String] = timeFieldMapState.values().iterator().toSet
//          counts = fieldSet.size
//        }else {
//          timeFieldListState.add(key)
//          val list: List[String] = timeFieldListState.get().iterator().toList
//          counts=list.size
//        }
//        out.collect(input+"|"+counts)
//      }
//
//      override def onTimer(timestamp: Long,
//                           ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
//                           out: Collector[String]): Unit = {
//        val key=ctx.getCurrentKey+"_"+timestamp
//        if(is_distinct){
//          if(timeFieldMapState.keys()!=null){
//            timeFieldMapState.remove(key)
//          }
//
//        }
//        else{
//          if(timeFieldListState!=null){
//            val resultList= new util.LinkedList[String]()
//            val it: util.Iterator[String] =timeFieldListState.get().iterator()
//            while(it.hasNext){
//              val next=it.next()
//              if(next!=key){
//                resultList.add(next)
//              }
//            }
//            timeFieldListState.update( resultList)
//          }
//
//        }
//
//
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
//      .uid(context.genOperatorUid("1"))
//
//
//    val output_stream: DataStream[Row] = transStringStreamToOneRowStream(resultDS)
//    output_stream
//  }
//}
