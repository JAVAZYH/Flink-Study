package sql.transform.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/7/5
 * \* Time: 11:06
 * \
 */
public class TumbleWindowEventTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
//                new Configuration().set(RestOptions.PORT,8082)
//        );
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //可以配置窗口触发器
        /**
         * watermark到达窗口结束前的发射策略是否开启：table.exec.emit.early-fire.enabled，默认false
         * table.exec.emit.early-fire.delay，窗口结束前的发射间隔，单位毫秒。=0，无间隔，>0 间隔时间，<0 非法值。无默认值
         */
        TableConfig config = tableEnv.getConfig();
        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
        config.getConfiguration().setString("table.exec.emit.early-fire.delay", "0s");

        SingleOutputStreamOperator<Row> reportDS = env.socketTextStream("192.168.126.129", 9999).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(2);
                row.setField(0,Long.parseLong(split[0]));
                row.setField(1, split[1]);
                return row;
            }
        }).returns(
                new RowTypeInfo(
                        Types.LONG,
                        Types.STRING
                )
        );
        tableEnv.createTemporaryView("report_table", reportDS, "report_time,report_id,proctime.proctime");
        /**
         * 统计每天被举报人数
         */
        Table table = tableEnv.sqlQuery("select count(distinct report_id),report_time from report_table group by tumble(proctime,interval '5' second),report_time");
        tableEnv.toRetractStream(table, Row.class).print();
        env.execute();
    }

}