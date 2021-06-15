package netease;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/4
 * \* Time: 11:07
 * \
 * 处罚关联举报，未关联到的举报不输出，举报数据默认缓存两天
 */
public class GmJoinReport {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> inputDS1 = env.socketTextStream("192.168.126.129", 9999 );

        DataStream<String> inputDS2 = env.socketTextStream("192.168.126.129", 9998);


        tableEnv.createTemporaryView("punish",inputDS1);
        tableEnv.createTemporaryView("report",inputDS1);

        tableEnv.executeSql("" +
                "");






    }
}