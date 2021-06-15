package datastream_api.common
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import scala.collection.mutable
/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2021/3/31
  * \* Time: 16:52
  * \*/
object QueueStateTest {
  def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        env.setParallelism(1)

        val inputStream: DataStream[DataTest] = env.socketTextStream("localhost", 7777)
          .map(data => {
            val dataArray: Array[String] = data.split("\\|")
            DataTest(dataArray(0).trim, dataArray(1).trim)
          })
        val result: DataStream[String] = inputStream.keyBy(x => {
          val key: String = x.key
          key
        })
          .process(new CalcQueue)

        result.print("value")
        env.execute("judgeResult")
      }

    }

    case class DataTest(key: String, uin: String)

    class CalcQueue extends KeyedProcessFunction[String, DataTest, String] {
      private var testValueState: ValueState[mutable.Queue[String]] = _

      override def open(parameters: Configuration): Unit = {
        testValueState = getRuntimeContext.getState(new ValueStateDescriptor[mutable.Queue[String]]("hhhh", classOf[mutable.Queue[String]]))
      }

      override def processElement(input: DataTest, context: KeyedProcessFunction[String, DataTest, String]#Context, collector: Collector[String]): Unit = {
        var testQueue: mutable.Queue[String] = testValueState.value()
        if(testQueue==null)testQueue=mutable.Queue[String]()
        if (!testQueue.contains(input.key)) {
          while (testQueue.size > 5) {
            testQueue.dequeue()
          }
          testQueue.enqueue(input.key)
        }
        val builder: StringBuilder = new StringBuilder
        val str: String = builder.mkString("-")
        collector.collect(str)
        val ts: Long = System.currentTimeMillis()
        context.timerService().registerProcessingTimeTimer(ts)
        context.timerService().registerProcessingTimeTimer(ts)
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, DataTest, String]#OnTimerContext, out: Collector[String]): Unit = {
        println("get======="+timestamp)
        testValueState.clear()
      }

}