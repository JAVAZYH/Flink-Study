package sql.transform.join;

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
 * \* Date: 2021/6/22
 * \* Time: 10:24
 * \
 */
public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        //目前还有点问题
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Row> reportDS = env.socketTextStream("192.168.126.129", 9999).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(2);
                row.setField(0, Long.parseLong(split[0]));
                row.setField(1, split[1]);
                return row;
            }
        }).returns(
                new RowTypeInfo(
                        Types.STRING,
                        Types.STRING
                )
        );
//                .assignTimestampsAndWatermarks(
//                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                .withTimestampAssigner((element,recodTime)->element)
//        )

        SingleOutputStreamOperator<Row> punishDS = env.socketTextStream("192.168.126.129", 9998).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(2);
                row.setField(0, Long.parseLong(split[0]));
                row.setField(1, split[1]);
                return row;
            }
        }).returns(
                new RowTypeInfo(
                        Types.LONG,
                        Types.STRING
                )
        );


        tableEnv.createTemporaryView("report_table",reportDS,"report_time,report_id");
        tableEnv.createTemporaryView("punish_table",punishDS,"punish_time,punish_id");

        //举报1小时之后，有处罚的数据才是有效举报
//        Table table = tableEnv.sqlQuery("select * from report_table");
//        Table punish_table = tableEnv.sqlQuery("select * from punish_table");
        Table resTable = tableEnv.sqlQuery("select *\n" +
                "from report_table a,punish_table b\n" +
                "where a.report_id=b.punish_id \n" +
                "and a.report_time between b.punish_time - interval '1' hour and a.report_time");

//        tableEnv.toRetractStream(table,Row.class).print("report");
//        tableEnv.toRetractStream(punish_table,Row.class).print("punish");
        tableEnv.toRetractStream(resTable,Row.class).print("res");
        env.execute();
    }

}