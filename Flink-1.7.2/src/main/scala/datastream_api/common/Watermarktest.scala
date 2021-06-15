package datastream_api.common

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object WatermarkTest {

  def main(args: Array[String]): Unit = {

    val hostName = "9.134.217.5"
    val port =9999

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
//    env.getConfig.setAutoWatermarkInterval(3000)


    val input = env.socketTextStream(hostName,port)
    val ouputTag=new OutputTag[(String,Long)]("side")

    val inputMap = input.map(f=> {
      val arr = f.split(',')
      val code = arr(0)
      val time = arr(1).toLong
      (code,time)
    })

    val watermark = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)] {

      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 10000L//最大允许的乱序时间是10s

      var a : Watermark = null

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        a
      }

      override def extractTimestamp(t: (String,Long), l: Long): Long = {
        val timestamp = t._2
        //找到最大的事件时间
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)

        /**
          * 抽取timestamp生成watermark。并打印（code，time，
          * 格式化的time，currentMaxTimestamp，
          * currentMaxTimestamp的格式化时间，watermark时间）。
          */
        println("timestamp:" + t._1 +","+ t._2 + "|"
          +format.format(t._2) +","+  currentMaxTimestamp + "|"+
          format.format(currentMaxTimestamp) + ","+ a.toString)
        timestamp
      }
    })

    /**
      *assignTimestampsAndWatermarks，周期性的生成watermark，默认是200毫秒，可以使用env.getConfig.setAutoWatermarkInterval(3000)更改
      * allowedLateness表示即使watermark到达了还允许窗口再等待1秒
      * sideOutputLateData表示窗口丢弃后没有参与计算的数据，放入到了侧输出流中
      */
    val window = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .allowedLateness(Time.seconds(1))
      .sideOutputLateData(ouputTag)
      .apply(new WindowFunctionTest)

    window.print()
    window.getSideOutput(ouputTag).print("side")

    env.execute()
  }

  class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String,String),String,TimeWindow]{

    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int,String,String,String,String)]): Unit = {
      val list = input.toList.sortBy(_._2)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      /**
        *
        * event time每隔3秒触发一次窗口，输出（code，窗口内元素个数，
        * 窗口内最早元素的时间，窗口内最晚元素的时间，
        * 窗口自身开始时间，窗口自身结束时间）
        */
      out.collect(key,input.size,
        format.format(list.head._2),format.format(list.last._2),
        format.format(window.getStart),format.format(window.getEnd))
    }

  }


}