package dataStreamAPI.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/14
 * \* Time: 3:22 下午
 * \
 */
public class RepartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism( 4 );
        DataStreamSource<String> inputDS = env.socketTextStream( "hadoop120", 9999 );

        //一共有8种改变分区的方式
        inputDS.keyBy( data->data ).print("keyby");
        inputDS.shuffle().print("shuffle");
        //rebalance是对每个并行度都做轮询，rescale是对每个并行度划分一个范围，然后范围内轮询，这样做的传输的开销更小
        inputDS.rebalance().print("rebalance");
        inputDS.rescale().print("rescale");
        inputDS.global().print("global");
//        inputDS.broadcast().print("broadcast");
//        inputDS.forward().print();
        //自定义方式改变分区

        env.execute();

    }
}