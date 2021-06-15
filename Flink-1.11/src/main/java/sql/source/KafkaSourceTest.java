package sql.source;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/5/27
 * \* Time: 14:30
 * \
 */
public class KafkaSourceTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table source_sensor(id string,ts bigint,vc int)\n" +
                "with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='ares',\n" +
                "'properties.bootstrap.servers'='192.168.126.129:9092',\n" +
                "'properties.group.id'='ares',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'format'='json'\n" +
                ")");

        Table resTable = tableEnv.sqlQuery("select * from source_sensor");

        tableEnv.toAppendStream(resTable, Row.class).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        tableEnv.toAppendStream(inputTable, Row.class)
    }
}