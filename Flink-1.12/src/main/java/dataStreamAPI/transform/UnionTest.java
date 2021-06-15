package dataStreamAPI.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/14
 * \* Time: 10:57 上午
 * \
 */
public class UnionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism( 1 );
        DataStreamSource<Integer> inputDS1 = env.fromElements( 1, 2, 3, 4 );
        DataStreamSource<Integer> inputDS2 = env.fromElements( 1, 2, 3, 4 );
        DataStreamSource<Integer> inputDS3 = env.fromElements( 1, 2, 3, 4 );

        inputDS1.union( inputDS2,inputDS3).print();


        env.execute();
    }
}