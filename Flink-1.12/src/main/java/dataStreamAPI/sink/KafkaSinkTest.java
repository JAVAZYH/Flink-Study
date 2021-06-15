package dataStreamAPI.sink;

import bean.WaterSensor;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/14
 * \* Time: 4:09 下午
 * \
 */
public class KafkaSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism( 1 );

        DataStreamSource<String> inputDS = env.socketTextStream( "hadoop120", 9999 );

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = inputDS.map( new MapFunction<String, WaterSensor>() {
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split( "," );
                return new WaterSensor( split[0], Long.parseLong( split[1] ), Integer.parseInt( split[2] ) );
            }
        } );

        Properties properties = new Properties();
        properties.setProperty( "bootstrap.servers","hadoop120:9092" );

        waterSensorDS.map( new MapFunction<WaterSensor, String>() {

            public String map(WaterSensor waterSensor) throws Exception {
                return JSON.toJSONString( waterSensor );
            }
        } ).addSink( new FlinkKafkaProducer<String>( "flink",new SimpleStringSchema(),properties ) );



        env.execute();


    }

}