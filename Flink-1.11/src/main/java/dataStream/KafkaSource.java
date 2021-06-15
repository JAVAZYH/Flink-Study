package dataStream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/13
 * \* Time: 4:13 下午
 * \
 */
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.126.129:9092" );
        properties.put( ConsumerConfig.GROUP_ID_CONFIG,"flink01");
        DataStreamSource<String> kafkaDS = env.addSource( new FlinkKafkaConsumer<String>( "ares", new SimpleStringSchema(), properties ) );

        kafkaDS.print();
        env.execute();
    }
}