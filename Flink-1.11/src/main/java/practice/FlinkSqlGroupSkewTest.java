package practice;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/7/2
 * \* Time: 16:36
 * \
 * flink 统计uv 处理数据倾斜
 * 统计当天不同游戏下的uv，并且处理数据倾斜
 *
 * 1.给游戏id拼接后缀用户id局部去重统计
 * 2.然后去除后缀做全局去重累加
 */
public class FlinkSqlGroupSkewTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration().set(RestOptions.PORT,8082)
        );
        env.setParallelism(4);
        env.disableOperatorChaining();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Row> reportDS = env.socketTextStream("192.168.126.129", 9999).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(3);
                row.setField(0,split[0]);
                row.setField(1,split[1]);
                row.setField(2, split[2]);
                return row;
            }
        }).returns(
                new RowTypeInfo(
                        Types.STRING,
                        Types.STRING,
                        Types.STRING
                )
        );


        tableEnv.createTemporaryView("report_table", reportDS, "game_id,report_time,report_id");


//        Table table = tableEnv.sqlQuery("select game_id,count(distinct report_id),max(report_time) from report_table " +
//                "group by game_id");
        Table table = tableEnv.sqlQuery("with rand_table as(\n" +
                "select game_id,report_id,\n" +
                "concat(game_id,'~',substr(report_id,0,1)) as game_id_report_id,\n" +
                "report_time\n" +
                "from report_table\n" +
                "),one_table as (\n" +
                "select game_id_report_id,count(distinct report_id) as cnt,max(report_time) as report_time\n" +
                "from rand_table \n" +
                "group by game_id_report_id\n" +
                "),group_table as(\n" +
                "select\n" +
                "split_index(game_id_report_id,'~',0) as game_id,cnt,report_time\n" +
                "from one_table\n" +
                ")\n" +
                "select\n" +
                "game_id,sum(cnt) as report_cnt,max(report_time)\n" +
                "from group_table\n" +
                "group by game_id");

        tableEnv.toRetractStream(table, Row.class).print();
        env.execute();
    }
}