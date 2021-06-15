package dataStreamAPI.source;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Arrays;
import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/13
 * \* Time: 3:25 下午
 * \
 */
public class SimpleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism( 1 );

        //2.准备集合
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));

        //3.从集合读取数据
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromCollection(waterSensors);

        //4.打印
        waterSensorDataStreamSource.print();

        //5.执行任务
        env.execute();


    }
}