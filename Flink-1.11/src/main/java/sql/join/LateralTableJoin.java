package sql.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/23
 * \* Time: 17:50
 * \
 */
public class LateralTableJoin {
    public static void main(String[] args) {
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
        tableEnv.createTemporaryView("report_table",reportDS,"report_time,report_id");





    }
}