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
 * flink join 处理数据倾斜，join一般不会出现数据倾斜
 * 如果倾斜需要对小表做扩容
 */
public class FlinkSqlJoinSkewTest {
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
                Row row = new Row(2);
                row.setField(0,split[0]);
                row.setField(1, split[1]);
                return row;
            }
        }).returns(
                new RowTypeInfo(
                        Types.STRING,
                        Types.STRING
                )
        );

        SingleOutputStreamOperator<Row> punishDS = env.socketTextStream("192.168.126.129", 9998).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(2);
                row.setField(0, split[0]);
                row.setField(1, split[1]);
                return row;
            }
        }).returns(
                new RowTypeInfo(
                        Types.STRING,
                        Types.STRING
                )
        );

        tableEnv.createTemporaryView("punish_table",punishDS,"punish_time,punish_id");
        tableEnv.createTemporaryView("report_table", reportDS, "report_time,report_id");


        Table table = tableEnv.sqlQuery("select report_id,report_time,punish_id,punish_time from report_table a " +
                "left join punish_table b on a.report_id=b.punish_id ");

        tableEnv.toRetractStream(table, Row.class).print();
        env.execute();
    }
}