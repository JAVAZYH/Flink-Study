package sql.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/2
 * \* Time: 10:51
 * \
 *
 * bug:FLINK-20961,使用事件时间时如果不指定时间戳和watermark会出现异常
 * Caused by: java.lang.NullPointerException
 * 	at SourceConversion$3.processElement(Unknown Source
 *
 */
public class EventTimeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



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
        )
                .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Row>() {
                    @Override
                    public long extractTimestamp(Row element, long recordTimestamp) {
                        return Long.parseLong(element.getField(0).toString())*1000;
                    }
                })
        );

        tableEnv.createTemporaryView("report_table", reportDS, "report_time.rowtime,report_id");
        Table table = tableEnv.sqlQuery("select * from report_table");
        tableEnv.toRetractStream(table, Row.class).print();
        env.execute();


//        //流转表定义事件时间
//        SingleOutputStreamOperator<WaterSensor> inputDS = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
//                new WaterSensor("sensor_1", 2000L, 20),
//                new WaterSensor("sensor_2", 3000L, 30),
//                new WaterSensor("sensor_1", 4000L, 40),
//                new WaterSensor("sensor_1", 5000L, 50),
//                new WaterSensor("sensor_2", 6000L, 60))
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs()));
//        //新增字段指定
//        Table table = tableEnv.fromDataStream(inputDS, $("id"), $("ts"), $("vc"), $("et").rowtime());
//        //已有字段指定
////        Table table = tableEnv.fromDataStream(inputDS, $("id"), $("ts").rowtime(), $("vc"));
//        table.execute().print();
//        env.execute();


        //注册表ddl指定
//        tableEnv.executeSql("create table sensor(id string,ts bigint,vc int,t as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss')),\n" +
//                "watermark for t as t - interval '5' second\n" +
//                ")\n" +
//                "with(\n" +
//                "'connector'='filesystem',\n" +
//                "'path'='E:\\Flink-Study-master\\Flink-1.11\\src\\main\\resources\\sensor.txt',\n" +
//                "'format'='csv'\n" +
//                ")");
//
//        tableEnv.sqlQuery("select * from sensor").execute().print();


    }
}