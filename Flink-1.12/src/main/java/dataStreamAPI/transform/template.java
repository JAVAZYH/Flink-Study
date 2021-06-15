package dataStreamAPI.transform;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/14
 * \* Time: 10:46 上午
 * \
 */
public class template {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism( 1 );

        DataStreamSource<String> inputDS = env.socketTextStream( "hadoop120", 9999 );

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = inputDS.map( new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split( "," );
                return new WaterSensor( split[0], Long.parseLong( split[1] ), Integer.parseInt( split[2] ) );
            }
        } );



        env.execute();


    }
}