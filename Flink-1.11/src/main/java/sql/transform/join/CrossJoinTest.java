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
 * \* Date: 2021/6/23
 * \* Time: 10:57
 * \
 */
public class CrossJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

////        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<Row> reportDS = env.socketTextStream("192.168.126.129", 9999).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(3);
                row.setField(0, Long.parseLong(split[0]));
                row.setField(0, split[0]);
                row.setField(1, split[1]);
                row.setField(2,split[2].split("\\|"));
                return row;
            }
        }).returns(
                new RowTypeInfo(
                        Types.STRING,
                Types.STRING,
                Types.OBJECT_ARRAY(Types.STRING)
                )
        );
        tableEnv.createTemporaryView("report_table",reportDS,"report_time,report_id,test");
        Table table = tableEnv.sqlQuery("select report_time,report_id,tag from report_table cross join unnest(test) as t(tag)");
//        Table table = tableEnv.sqlQuery("select * from report_table");
        tableEnv.toRetractStream(table,Row.class).print("res");
        env.execute();



//        List<Row> rows = Arrays.asList(
//                Row.of(1, new Row[]{Row.of("1a"), Row.of("1b")}),
//                Row.of(2, new Row[]{Row.of("2a"), Row.of("2b")}),
//                Row.of(3, new Row[]{Row.of("3a"), Row.of("3b")})
//        );
//
//        TypeInformation<?>[] types = new TypeInformation[]{Types.INT, Types.OBJECT_ARRAY(Types.ROW(Types.INT,Types.STRING))};
////        TypeInformation<?>[] types = new TypeInformation[]{Types.INT, ObjectArrayTypeInfo.getInfoFor(new RowTypeInfo(Types.INT, Types.STRING))};
//        String[] typeNames = new String[]{"a", "b"};
//
//        DataStream<Row> source = env
//                .fromCollection(rows)
//                .returns(new RowTypeInfo(types, typeNames));
//
//        tableEnv.registerDataStream("source", source);
//
//        Table a = tableEnv.sqlQuery("select a,t.c from source,unnest(b) as t (c)");
//
//        tableEnv.toAppendStream(a, Row.class).print();

//        env.execute();
    }
}