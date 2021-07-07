package sql.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/7/2
 * \* Time: 14:14
 * \
 */
public class HDFSTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("hdfs://192.168.126.129:9000/test/");
        stringDataStreamSource.print();
        env.execute();
    }
}