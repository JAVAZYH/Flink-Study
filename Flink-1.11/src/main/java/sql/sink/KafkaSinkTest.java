package sql.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/5/27
 * \* Time: 15:29
 * \
 */
public class KafkaSinkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("create table sink_sensor(id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sink_sensor',"
                + "'properties.bozotstrap.servers' = '192.168.126.129',"
                + "'format' = 'json'"
                + ")");

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor");
    }
}