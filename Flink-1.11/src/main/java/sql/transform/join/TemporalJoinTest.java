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
 * \* Date: 2021/6/25
 * \* Time: 16:30
 * \
 *
 *
 */
public class TemporalJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
//                new Configuration().set(RestOptions.PORT,8082)
//        );
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
        tableEnv.createTemporaryView("report_table", reportDS, "report_time,report_id,proctime.proctime");
        tableEnv.executeSql("create table user_detail(\n" +
                "user_id string,\n" +
                "user_name string,\n" +
                "user_addr string\n" +
                ")with(\n" +
                "'connector'='jdbc',\n" +
                "'url'='jdbc:mysql://192.168.126.129:3306/flink_sql?useSSL=false',\n" +
                "'username'='root',\n" +
                "'password'='123456',\n" +
                "'table-name'='user_detail',\n" +
                "'lookup.cache.max-rows'='1',"+
                "'lookup.cache.ttl'='60s',"+
                "'lookup.max-retries'='1'"+
                ")");
        //jdbc 相关配置   -- jdbc作为维表的时候，缓存时间。cache默认未开启。
        //  'connector.lookup.cache.ttl' = '60s',
        //
        //  --  jdbc作为维表的时候，缓存的最大行数。cache默认未开启,这里指的是缓存的mysql中的数据条数
        //  'connector.lookup.cache.max-rows' = '100000',
        //
        //  -- jdbc作为维表的时候，如果查询失败，最大查询次数
        //  'connector.lookup.max-retries' = '3',


        //HBase维表关联
        // CREATE TABLE MyUserTable (
        //  hbase_rowkey_name rowkey_type,
        //  hbase_column_family_name1 ROW<...>,
        //  hbase_column_family_name2 ROW<...>
        //) WITH (
        //  'connector.type' = 'hbase',
        //  'connector.version' = '1.4.3',
        //
        //  -- hbase 表名称
        //  'connector.table-name' = 'hbase_table_name',  -- required: hbase table name
        //
        //  -- zk地址
        //  'connector.zookeeper.quorum' = 'localhost:2181',
        //  -- zk 根节点
        //  'connector.zookeeper.znode.parent' = '/base',
        //
        //  -- buffer 缓存大小。默认 2mb。
        //  'connector.write.buffer-flush.max-size' = '10mb',
        //
        //  -- 缓冲的最大记录数，无默认值
        //  'connector.write.buffer-flush.max-rows' = '1000',
        //
        //  -- flush 时间间隔，默认为0，表示理解刷新到hbase，无缓冲。
        //  'connector.write.buffer-flush.interval' = '2s'
        //)


        Table table = tableEnv.sqlQuery("select *\n" +
                "from  report_table as a\n" +
                "left join user_detail for system_time as of  a.proctime as b\n" +
                "on a.report_id=b.user_id");
        tableEnv.toRetractStream(table, Row.class).print();
        env.execute();
    }
}