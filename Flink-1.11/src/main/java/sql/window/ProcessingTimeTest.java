package sql.window;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/2
 * \* Time: 10:32
 * \
 */
public class ProcessingTimeTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //在流转表时定义处理时间字段
//        DataStreamSource<WaterSensor> inputDS = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
//                new WaterSensor("sensor_1", 2000L, 20),
//                new WaterSensor("sensor_2", 3000L, 30),
//                new WaterSensor("sensor_1", 4000L, 40),
//                new WaterSensor("sensor_1", 5000L, 50),
//                new WaterSensor("sensor_2", 6000L, 60));
//        Table inputTable = tableEnv.fromDataStream(inputDS, $("id"), $("ts"), $("vc"), $("pt").proctime());
//        tableEnv.sqlQuery("select id,ts,vc,pt from "+inputTable).execute().print();

        //在创建表ddl定义
        tableEnv.executeSql("create table sensor(id string,ts bigint,vc int,pt_time as proctime())\n" +
                "with(\n" +
                "'connector'='filesystem',\n" +
                "'path'='E:\\Flink-Study-master\\Flink-1.11\\src\\main\\resources\\sensor.txt',\n" +
                "'format'='csv'\n" +
                ")");

        tableEnv.executeSql("select * from sensor").print();




    }
}