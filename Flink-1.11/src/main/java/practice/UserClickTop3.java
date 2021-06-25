package practice;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/24
 * \* Time: 17:49
 * \
 * 统计每小时点击量top3的商品
 *
 *-- flink_sql.product_hot3_hour definition
 *sql建表语句
 * CREATE TABLE `product_hot3_hour` (
 *   `datetime_hour` varchar(100) NOT NULL,
 *   `productID` varchar(100) DEFAULT NULL,
 *   `user_cnt` bigint DEFAULT NULL,
 *   `rk` bigint NOT NULL,
 *   PRIMARY KEY (`datetime_hour`,`rk`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 */
public class UserClickTop3 {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        // Source DDL
        String sourceDDL ="create table source_kafka( userID String, eventType String,eventTime String,productID String)\n" +
                "with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='user_click_topic',\n" +
                "'properties.bootstrap.servers'='192.168.126.129:9092',\n" +
                "'properties.group.id'='ares',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'format'='json'\n" +
                ")";

        tableEnv.executeSql(sourceDDL);

        String sql="with user_gruop_tbl as (\n" +
                "SELECT SUBSTRING(eventTime,1,13) AS datetime_hour,productID,count(userID) as user_cnt\n" +
                "from source_kafka WHERE eventType='click'\n" +
                "GROUP by SUBSTRING(eventTime,1,13),productID\n" +
                "),user_rk_tbl as(\n" +
                "SELECT datetime_hour,productID,user_cnt,ROW_NUMBER() OVER(PARTITION by datetime_hour ORDER by user_cnt desc) as rk\n" +
                "from user_gruop_tbl\n" +
                ")\n" +
                "SELECT datetime_hour,productID,user_cnt,rk from user_rk_tbl WHERE rk<=3";
        Table table = tableEnv.sqlQuery(sql);

        String sinkDDL = "create table sink_mysql(\n" +
                "datetime_hour STRING,\n" +
                "productID STRING,\n" +
                "user_cnt BIGINT,\n" +
                "rk BIGINT,\n" +
                "primary key(datetime_hour,rk) NOT ENFORCED "+
                ") with (\n" +
                "  'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://192.168.126.129:3306/flink_sql?useSSL=false',\n" +
                "   'username' = 'root',\n" +
                "   'password'='123456',\n" +
                "'table-name'='product_hot3_hour'"+
                ")";

        tableEnv.executeSql(sinkDDL);
        table.executeInsert("sink_mysql");
        tableEnv.toRetractStream(table, Row.class).print();
        streamEnv.execute();
    }
}