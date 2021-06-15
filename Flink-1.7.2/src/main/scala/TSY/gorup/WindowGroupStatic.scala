package TSY.gorup

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2021/1/20
  * \* Time: 11:59
  * TSY流量控制-窗口聚合
  * 以窗口方式聚合数据，减少flink输出的数据量
  * 以时间+key（分钟整点时间_列名_列值）作为key，每分钟输出一条,减少数据量。
  * \*/
object WindowGroupStatic {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10,CheckpointingMode.EXACTLY_ONCE)

    val inputDataStream: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    val main_key_pos="1"
    val windowSize="1".toLong

    val resultDS: DataStream[String] = inputDataStream.keyBy(str => {
      val arr: Array[String] = str.split('|')
      val mainArr: Array[String] = main_key_pos.split('|')
      var key = ""
      for (ele <- mainArr) {
        key += "_"+arr(ele.toInt - 1)
      }
      key
    })
      .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSize)))
      .reduce((str1,str2)=>str2)

    resultDS.print()

    env.execute()





  }

}