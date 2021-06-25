package sql.group;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/24
 * \* Time: 16:38
 * \
 * 使用topn必须配合 where语句使用，否则会出现异常
 */
public class TopNTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
//                new Configuration().set(RestOptions.PORT,8082)
//        );
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Row> reportDS = env.socketTextStream("192.168.126.129", 9999).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(3);
                row.setField(0,split[0]);
                row.setField(1, split[1]);
                row.setField(2, Long.parseLong(split[2]));
                return row;
            }
        }).returns(
                new RowTypeInfo(
                        Types.STRING,
                        Types.STRING,
                        Types.LONG
                )
        );
//                .assignTimestampsAndWatermarks(
//                WatermarkStrategy
//                        .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Row>() {
//                            @Override
//                            public long extractTimestamp(Row element, long recordTimestamp) {
//                                return Long.parseLong(element.getField(0).toString())*1000;
//                            }
//                        })
//        );

        tableEnv.createTemporaryView("report_table", reportDS, "report_time,report_id,report_cnt");
        Table table = tableEnv.sqlQuery("select * from(" +
                "select *,row_number() over(partition by report_time,report_id order by report_cnt) as rk from report_table) where rk <=3");
        tableEnv.toRetractStream(table, Row.class).print();
        env.execute();

    }
}