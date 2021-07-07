package sql.transform.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import sql.transform.function.udf.StringToArrayFunc;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/24
 * \* Time: 14:39
 * \
 */
public class BuildInFuncTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        tableEnv.createTemporaryView("report_table", reportDS, "report_time,report_id");

//        tableEnv.createTemporaryFunction("json_str", JsonStrFunc.class);
        //json_str对map=2的元素只会保留key
//        Table table = tableEnv.sqlQuery("select json_str('report_id',report_id," +
//                "'tag',tag) from (" +
//                "select report_id,collect(all report_time) as tag from report_table group by report_id)");

//        Table table = tableEnv.sqlQuery("select report_id," +
//                "tag from (" +
//                "select report_id,collect(all report_time) as tag from report_table group by report_id)");

        //listagg 和collect_set类似
        tableEnv.createTemporaryFunction("str_arr", StringToArrayFunc.class);
        Table table = tableEnv.sqlQuery("select report_id,tag from (" +
                "select report_id,str_arr(listagg(report_time)) as tag from report_table group by report_id)");

        //lead,lag取组内最新
//        Table table = tableEnv.sqlQuery("select report_id,tag from (" +
//                "select report_id,LAG(report_time) as tag from report_table group by report_id )");

        //LAST_VALUE有序集合最后一个值,FIRST_VALUE第一个值
//        Table table = tableEnv.sqlQuery("select report_id,tag from (" +
//                "select report_id,FIRST_VALUE(report_time) as tag from report_table group by report_id )");



        tableEnv.toRetractStream(table,Row.class).print();
        env.execute();
    }
}