package dataStreamAPI.sink;

import bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: YY
 * \* Date: 2021/4/29
 * \* Time: 15:40
 * \
 */
public class MyMySQLSink {
    public static void main(String[] args) throws Exception {
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.fromCollection(waterSensors)
                .addSink(new RichSinkFunction<WaterSensor>() {

                    private PreparedStatement ps;
                    private Connection conn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "000000");
                        ps = conn.prepareStatement("insert into sensor values(?, ?, ?)");
                    }

                    @Override
                    public void close() throws Exception {
                        ps.close();
                        conn.close();
                    }

                    @Override
                    public void invoke(WaterSensor value, Context context) throws Exception {
                        ps.setString(1, value.getId());
                        ps.setLong(2, value.getTs());
                        ps.setInt(3, value.getVc());
                        ps.execute();
                    }
                });


        env.execute();

    }
}