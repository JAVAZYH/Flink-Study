package sql.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/17
 * \* Time: 11:19
 * \
 */


public class SQLIdelTimeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableConfig config = tableEnv.getConfig();
        config.setIdleStateRetentionTime(Time.seconds(1),Time.minutes(6));
//                .map(new MapFunction<String, Row>() {
//                    @Override
//                    public Row map(String s) throws Exception {
//                        String[] split = s.split("\\|");
//                        return null;
//                    }
//                })
        SingleOutputStreamOperator<Row> sourceDS = env.socketTextStream("192.168.126.129", 9999)
                .map(new MapFunction<String, Row>() {
                    @Override
                    public Row map(String s) throws Exception {
                        String[] split = s.split("\\|");
                        Row row = new Row(3);
                        row.setField(0, split[0]);
                        row.setField(1, split[1]);
                        row.setField(2, split[2]);
                        return row;
                    }
                }).returns(
                        new RowTypeInfo(Types.STRING, Types.STRING, Types.STRING)
                );


//        Table table = tableEnv.fromDataStream(sourceDS,$("rerole_id"),$("role_id"),$("log_time"));
//        Table table = tableEnv.fromDataStream(sourceDS);
//        tableEnv.createTemporaryView("test",table);
        tableEnv.createTemporaryView("test",sourceDS,"rerole_id,role_id,log_time");
//        sourceDS.print();

        Table table1 = tableEnv.sqlQuery("select rerole_id,count(distinct role_id) as idcnt,max(log_time) from test group by rerole_id");
//        Table table1 = tableEnv.sqlQuery("select role_id from test");

        tableEnv.toRetractStream(table1, Row.class).print();

        env.execute();

    }
}