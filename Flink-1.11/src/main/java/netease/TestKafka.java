package netease;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/7/1
 * \* Time: 17:32
 * \
 */
public class TestKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        tableEnv.executeSql("create table source_sensor(id string,ts bigint,vc int)\n" +
//                "with(\n" +
//                "'connector'='kafka',\n" +
//                "'topic'='aresTopic',\n" +
//                "'properties.bootstrap.servers'='192.168.126.129:9092',\n" +
//                "'properties.group.id'='ares',\n" +
//                "'scan.startup.mode'='latest-offset',\n" +
//                "'format'='csv'\n" +
//                ")");
        tableEnv.executeSql("create table test(logStr string)\n" +
                "with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='test01_opd_topic_zyh',\n" +
                "'properties.bootstrap.servers'='test01.brokers.canal.netease.com:9092',\n" +
                "'properties.group.id'='test-ares',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'format'='csv'\n" +
                ")");



//        Table resTable = tableEnv.sqlQuery("select * from source_sensor");
        Table resTable2 = tableEnv.sqlQuery("select * from test");

//        tableEnv.toAppendStream(resTable, Row.class).print("resTable>>>");
        tableEnv.toAppendStream(resTable2, Row.class).print("resTable2>>");

        env.execute();
    }
}