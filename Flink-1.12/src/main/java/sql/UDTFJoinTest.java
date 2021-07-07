package sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import sql.transform.join.MySplit;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/23
 * \* Time: 17:50
 * \
 */


public class UDTFJoinTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Row> reportDS = env.fromElements(
                "2021-06-01 10:00:00,aaa,111|222|333",
                "2021-06-01 10:00:00,bbb,111|222|333",
                "2021-06-01 10:00:00,ccc,111|222|333",
                "2021-06-01 10:00:00,ddd,111|222|333",
                "2021-06-01 10:00:00,eee,111|222|333",
                "2021-06-01 10:00:00,fff,111|222|333"
        ).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(3);
                row.setField(0, split[0]);
                row.setField(1, split[1]);
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

        tableEnv.createTemporaryView("report_table",reportDS,"report_time,report_id,report_content");
        tableEnv.createTemporaryFunction("my_split", MySplit.class);
//        Table table = tableEnv.sqlQuery("select *  from report_table  ");
        Table table = tableEnv.sqlQuery("select *  from report_table join  lateral table(my_split(report_content)) ");
        tableEnv.toRetractStream(table,Row.class).print();
        env.execute();
    }
}