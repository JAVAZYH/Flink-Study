package dataStreamAPI.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/14
 * \* Time: 10:46 上午
 * \
 */
public class ShuffleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements( 1,2,3,5 ).shuffle().print();


        env.execute();


    }
}