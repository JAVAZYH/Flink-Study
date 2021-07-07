package sql.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/2
 * \* Time: 16:33
 * \
 * 目前还有问题，不能连接到hadoop 需要配置本地hadoop环境变量,https://www.jianshu.com/p/a65a95108620
 */
public class SinkHiveTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStreamSource<UserInfo> dataStream = env.addSource(new MySource());


//        String name            = "myhive";  // Catalog 名字
//        String defaultDatabase = "default"; // 默认数据库
//        String hiveConfDir     = "E:\\Flink-Study-master\\Flink-1.11\\src\\main\\resources\\"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录
////        String hiveConfDir     = "hive-site.xml"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录
//        String version="2.3.2";
//
//// 1. 创建HiveCatalog
//        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);
//// 2. 注册HiveCatalog
//        tEnv.registerCatalog(name, hive);
//// 3. 把 HiveCatalog: myhive 作为当前session的catalog
//        tEnv.useCatalog(name);
//        tEnv.useDatabase("default");

        //构造hive catalog
        String name = "myhive";      // Catalog名称，定义一个唯一的名称表示
        String defaultDatabase = "default";  // 默认数据库名称
        String hiveConfDir = "E:/Flink-Study-master/Flink-1.11/src/main/resources/";  // hive-site.xml路径
        String version = "2.3.2";       // Hive版本号


//        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
//        tEnv.registerCatalog("myhive", hive);
//        tEnv.useCatalog("myhive");
//        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tEnv.useDatabase("default");

        tEnv.createTemporaryView("users", dataStream);


        String hiveSql = "CREATE external TABLE fs_table (\n" +
                "  user_id STRING,\n" +
                "  order_amount DOUBLE" +
                ") partitioned by (dt string,h string,m string) " +
                "stored as ORC " +
                "TBLPROPERTIES (\n" +
                "  'partition.time-extractor.timestamp-pattern'='$dt $h:$m:00',\n" +
                "  'sink.partition-commit.delay'='0s',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
                "  'sink.partition-commit.policy.kind'='metastore'" +
                ")";

//        String sink_sql="CREATE EXTERNAL TABLE `users`(\n" +
//                "  `user_id` string, \n" +
//                "  `order_amount` double)\n" +
//                "PARTITIONED BY ( \n" +
//                "  `dt` string, \n" +
//                "  `h` string, \n" +
//                "  `m` string)\n" +
//                "stored as ORC \n" +
//                "TBLPROPERTIES (\n" +
//                "  'sink.partition-commit.policy.kind'='metastore'，\n" +
//                "  'partition.time-extractor.timestamp-pattern'='$dt $h:$m:00'\n" +
//                ")";
        tEnv.executeSql(hiveSql);

        String insertSql = "insert into  fs_table SELECT user_id, order_amount, " +
                " DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH'), DATE_FORMAT(ts, 'mm') FROM users";
        tEnv.executeSql(insertSql);

//        tEnv.sqlQuery("select * from pokes limit 3").execute().print();

    }
    public static class MySource implements SourceFunction<UserInfo> {

        String userids[] = {
                "4760858d-2bec-483c-a535-291de04b2247", "67088699-d4f4-43f2-913c-481bff8a2dc5",
                "72f7b6a8-e1a9-49b4-9a0b-770c41e01bfb", "dfa27cb6-bd94-4bc0-a90b-f7beeb9faa8b",
                "aabbaa50-72f4-495c-b3a1-70383ee9d6a4", "3218bbb9-5874-4d37-a82d-3e35e52d1702",
                "3ebfb9602ac07779||3ebfe9612a007979", "aec20d52-c2eb-4436-b121-c29ad4097f6c",
                "e7e896cd939685d7||e7e8e6c1930689d7", "a4b1e1db-55ef-4d9d-b9d2-18393c5f59ee"
        };

        @Override
        public void run(SourceFunction.SourceContext<UserInfo> sourceContext) throws Exception {

            while (true) {
                String userid = userids[(int) (Math.random() * (userids.length - 1))];
                UserInfo userInfo = new UserInfo();
                userInfo.setUserId(userid);
                userInfo.setAmount(Math.random() * 100);
                userInfo.setTs(new Timestamp(System.currentTimeMillis()));
                sourceContext.collect(userInfo);
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {

        }
    }

    public static class UserInfo implements java.io.Serializable {
        private String userId;
        private Double amount;
        private Timestamp ts;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public Double getAmount() {
            return amount;
        }

        public void setAmount(Double amount) {
            this.amount = amount;
        }

        public Timestamp getTs() {
            return ts;
        }

        public void setTs(Timestamp ts) {
            this.ts = ts;
        }
    }

}