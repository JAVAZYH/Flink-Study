package table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;

public class CSVSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("vc",DataTypes.INT());

//        tableEnv.connect()

    }
}
