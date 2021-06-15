package sql.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/2
 * \* Time: 16:33
 * \
 * 目前还有问题，不能连接到hadoop 需要配置本地hadoop环境变量,https://www.jianshu.com/p/a65a95108620
 */
public class HiveTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String name            = "myhive";  // Catalog 名字
        String defaultDatabase = "default"; // 默认数据库
        String hiveConfDir     = "E:\\Flink-Study-master\\Flink-1.11\\src\\main\\resources\\"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录
//        String hiveConfDir     = "hive-site.xml"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录

// 1. 创建HiveCatalog
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
// 2. 注册HiveCatalog
        tEnv.registerCatalog(name, hive);
// 3. 把 HiveCatalog: myhive 作为当前session的catalog
        tEnv.useCatalog(name);
        tEnv.useDatabase("default");
        tEnv.sqlQuery("select * from pokes limit 3").execute().print();

    }
}