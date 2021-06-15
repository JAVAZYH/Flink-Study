package MyUtil

import java.util.{Properties, Random}

import Model.GlobalConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.kafka.clients.producer.KafkaProducer

class testSource extends SourceFunction[String] {
  var isRunning: Boolean = true
  val datas: List[String] = List("111", "222", "333", "444")
  private val random = new Random()
  var number: Long = 0

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (isRunning) {
      val index: Int = random.nextInt(4)
      ctx.collect(datas(index)+"|"+TimeUtil.timestampToString(System.currentTimeMillis()))
      number += 1
      Thread.sleep(500)
      if(number==5000){
        cancel()
      }
    }

  }
  override def cancel(): Unit = {
    isRunning = false
  }
}

object ProducerTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val properties=new Properties()
    //    properties.setProperty("bootstrap.servers", "9.134.92.193:9092")
    //    properties.setProperty("group.id","test")
    //    val input_ds: DataStream[String] = env
    //      .addSource(new FlinkKafkaConsumer010[String]
    //      ("topic_test", new SimpleStringSchema(), properties))
    //    input_ds.print()

    val input_ds: DataStream[String] = env.addSource(new testSource)
    //    input_ds.print()
    val myProducer = new FlinkKafkaProducer010[String](
      "hadoop120:9092,hadoop121:9092,hadoop122:9092",
      "topic_test",
      new SimpleStringSchema()
    )
    input_ds.addSink(myProducer)

    env.execute()

  }


}
