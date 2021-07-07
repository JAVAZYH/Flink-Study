package sql.transform.function;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;


//flink 1.11-2.11 版本有bug无法注册udtf，切换到1.12版本解决
public class AggFunctionTest {
    public static class VcAvgAcc {
        public Integer sum = 0;
        public Long count = 0L;
    }
    public static class VcAvg extends AggregateFunction<Double, VcAvgAcc> {

        // 返回最终的计算结果
        @Override
        public Double getValue(VcAvgAcc accumulator) {
            return accumulator.sum * 1.0 / accumulator.count;
        }

        // 初始化累加器
        @Override
        public VcAvgAcc createAccumulator() {
            return new VcAvgAcc();
        }

        // 处理输入的值, 更新累加器
        // 参数1: 累加器
        // 参数2,3,...: 用户自定义的输入值
        public void accumulate(VcAvgAcc acc, Integer vc) {
            acc.sum += vc;
            acc.count += 1L;
        }

    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        Table table = tEnv.fromDataStream(waterSensorStream);

// 在sql中使用
// 1. 注册表
        tEnv.createTemporaryView("t_sensor", table);
// 2. 注册函数
        tEnv.createTemporaryFunction("my_avg", new VcAvg());
// 3. sql中使用自定义聚合函数
        tEnv.sqlQuery("select id, my_avg(vc) from t_sensor group by id").execute().print();


    }



}
