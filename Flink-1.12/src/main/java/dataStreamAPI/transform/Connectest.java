package dataStreamAPI.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/14
 * \* Time: 10:57 上午
 * \
 */
public class Connectest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism( 1 );
        DataStreamSource<Integer> inputDS = env.fromElements( 1, 2, 3, 4 );
        //2.数据创建流
        DataStreamSource<String> stringDS = env.fromElements( "1","2","3");
        DataStreamSource<String> socketTextStream2 = env.fromElements( "1","2","3");

        //3.将socketTextStream2转换为Int类型
        SingleOutputStreamOperator<Integer> intDS = socketTextStream2.map(String::length);

        //4.连接两个流
        ConnectedStreams<String, Integer> connectedStreams = stringDS.connect(intDS);

        connectedStreams.map( new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value+"map1";
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value+"map2";
            }
        } ).print();
        
        env.execute();
    }
}