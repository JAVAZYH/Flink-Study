//package MyUtil
//
//import java.util.Properties
//
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import Model.GlobalConfig
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.kafka.clients.producer.KafkaProducer
//
//
//object ConsumerTest {
//  def main(args: Array[String]): Unit = {
//
//
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//        val properties=new Properties()
////        properties.setProperty("bootstrap.servers", "9.134.92.193:9092")
//        properties.setProperty("bootstrap.servers", "hadoop120:9092,hadoop121:9092,hadoop122:9092")
//        properties.setProperty("group.id","test")
//        val input_ds: DataStream[String] = env
//          .addSource(new FlinkKafkaConsumer010[String]
//          ("topic_test", new SimpleStringSchema(), properties))
//        input_ds.print()
//
//    env.execute()
//
//  }
//}