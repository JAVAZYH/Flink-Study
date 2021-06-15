package sql.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OverWindowTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'sensor.txt',"
                + "'format' = 'csv'"
                + ")");

//        tEnv.sqlQuery(
//                "select\n" +
//                        "id,vc,\n" +
//                        "sum(vc) over(partition by id order by t rows between 1 preceding and current row) as total\n"  +
//                        "from sensor"
//        ).execute()
//                .print();

        tEnv
                .sqlQuery(
                        "select " +
                                "id," +
                                "vc," +
                                "count(vc) over w, " +
                                "sum(vc) over w " +
                                "from sensor " +
                                "window w as (partition by id order by t rows between 1 PRECEDING and current row)"
                )
                .execute()
                .print();



    }
}
