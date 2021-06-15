package sql;

import bean.UserInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

public class MyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        List<UserInfo> users = Arrays.asList(
                new UserInfo("aaa", "a", 1),
                new UserInfo("bbb", "b", 2),
                new UserInfo("ccc", "c", 3)
        );
        DataStreamSource<UserInfo> inputDS = env.fromCollection(users);

        //未注册表
        Table table = tableEnv.fromDataStream(inputDS);
        Table resultTable = tableEnv.sqlQuery("select product,amount from " + table + " where user like '%a%'");

        //已注册表
        tableEnv.createTemporaryView("userInfo",inputDS);
        Table resultTable2 = tableEnv.sqlQuery("select user,product,amount from userInfo where user like '%a%' ");

        //窗口函数测试
//        tableEnv.sqlQuery(
//                "select id,"
//        )

        tableEnv.toAppendStream(resultTable2, Row.class).print();

        Schema schema = new Schema()
                .field("user", DataTypes.STRING())
                .field("product", DataTypes.STRING())
                .field("amount", DataTypes.BIGINT());




        env.execute();
    }
}
