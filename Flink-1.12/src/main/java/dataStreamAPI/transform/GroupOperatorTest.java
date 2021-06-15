package dataStreamAPI.transform;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/14
 * \* Time: 11:38 上午
 * \
 */
public class GroupOperatorTest {
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

        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy( new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        } );

        //max,maxby,min,minby,sum
        //计算最高水位线，当值相同时根据第一条走
//        keyedStream.max( "vc" ).print("max");
        //计算最高水位线，当数据相同时可以选择是否用最新的数据
//        keyedStream.maxBy( "vc",true ).print("maxby");

        keyedStream.reduce( new ReduceFunction<WaterSensor>() {
            public WaterSensor reduce(WaterSensor t1, WaterSensor t2) throws Exception {
                return new WaterSensor(t1.getId(),t2.getTs(),Math.max( t1.getVc(),t2.getVc() ));
            }
        } ).print();

        env.execute();


    }
}