//package TSY.join
//
//import java.text.SimpleDateFormat
//import java.util.Date
//import java.{lang, util}
//
//import org.apache.flink.api.common.functions.CoGroupFunction
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.util.Collector
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: aresyhzhang
//  * \* Date: 2020/11/3
//  * \* Time: 20:13
//  * \*/
//object LeftJoin { def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    //第一个流和第二个流的位置偏移
//    val  mainKeyPos1=4-1
//    val  mainKeyPos2=9-1
//    val timePos1=5-1
//    val timePos2=10-1
//    val length2=9
//    val timeValid=86400
//
//    //    val input_Dstream1 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\tlog.txt")
//    //    val input_Dstream2 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\kda.txt")
//
//    val input_Dstream1 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\tlogtest.txt")
//    val input_Dstream2 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\kdatest.txt")
//
//
//
//
//
//    //对局结算数据流
//    val keyed_DStream1 = input_Dstream1
//      .map(line => {
//        val data_src = line.split('|')
//        val head = data_src(0)
//        val body = data_src(1)
//        var body_list = body.split(',')
//        body_list = body_list.map(_.trim)
//        val wordid = head.split("worldid=")(1).split("&")(0)
//        val data_list = wordid +: body_list
//        val data = data_list.mkString("|")
//        data
//      })
//      .assignAscendingTimestamps(str => {
//        val timeStr = str.split('|')(timePos1)
//        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//        format.parse(timeStr).getTime
//      })
//
//
//    //kda结合谩骂数据流,对每条数据的末尾都加上时间字段
//    val keyed_DStream2 = input_Dstream2
//      .map(_+"|"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
//      .assignAscendingTimestamps(str => {
//      val timeStr = str.split('|')(timePos2)
//      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      format.parse(timeStr).getTime
//    })
//
//
//    val result: DataStream[String] = keyed_DStream1.coGroup(keyed_DStream2)
//      .where(str => str.split('|')(mainKeyPos1))
//      .equalTo(str => str.split('|')(mainKeyPos2))
//      .window(TumblingEventTimeWindows.of(Time.seconds(timeValid)))
//      //      .trigger(CountTrigger.of(1))
//      .apply(new CoGroupFunction[String, String, String] {
//      override def coGroup(it1: lang.Iterable[String], it2: lang.Iterable[String], out: Collector[String]): Unit = {
//        val leftIt: util.Iterator[String] = it1.iterator()
//        val rightIt: util.Iterator[String] = it2.iterator()
//        var flag = false
//        while (leftIt.hasNext) {
//          val leftStr: String = leftIt.next()
//          while (rightIt.hasNext) {
//            val rightStr: String = rightIt.next()
//            flag = true
//            out.collect(leftStr + "|" + rightStr)
//          }
//          if (!flag) {
//            out.collect(leftStr + "|" + Array.iterate("-1", length2)(_ => "-1").mkString("|"))
//          }
//        }
//      }
//    })
//    result
//        .print()
//    env.execute()
//
//
//
//
//  }
//
//}