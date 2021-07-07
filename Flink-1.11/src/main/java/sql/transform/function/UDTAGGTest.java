package sql.transform.function;


import bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/3
 * \* Time: 14:26
 * \
 * 表聚合函数，比如topN的时候可以使用,也可以在udagg都保存为状态
 */
public class UDTAGGTest {
//    public static class Top3Acc{
//        public Integer first= 0;
//        public Integer second=0;
//        public Integer three=0;
//
//    }
//
////    找到每个sensor最高的前三个温度值
//    public static class Top3 extends TableAggregateFunction<Tuple3<Integer,Integer,Integer>,Top3Acc> {
//
//        @Override
//        public Top3Acc createAccumulator() {
//            return new Top3Acc();
//        }
//
//        public void accumulate(Top3Acc acc, Integer vc) {
//            if (vc > acc.first) {
//                acc.second = acc.first;
//                acc.first = vc;
//            } else if (vc > acc.second) {
//                acc.second = vc;
//            } else if (vc > acc.three) {
//                acc.three = vc;
//            }
//        }
//
//
//    public void emitValue(Top3Acc acc, Collector<Tuple2<Integer, Integer>> out) {
//        if (acc.first != 0) {
//            out.collect(Tuple2.of(acc.first, 1));
//        }
//
//        if (acc.second != 0) {
//            out.collect(Tuple2.of(acc.second, 2));
//        }
//        if (acc.three != 0) {
//            out.collect(Tuple2.of(acc.three, 3));
//        }
//
//    }
//
//}


    // 累加器
    public static class Top2Acc {
        public Integer first = Integer.MIN_VALUE; // top 1
        public Integer second = Integer.MIN_VALUE; // top 2
    }

    // Tuple2<Integer, Integer> 值和排序
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Acc> {

        @Override
        public Top2Acc createAccumulator() {
            return new Top2Acc();
        }

        public void accumulate(Top2Acc acc, Integer vc) {
            if (vc > acc.first) {
                acc.second = acc.first;
                acc.first = vc;
            } else if (vc > acc.second) {
                acc.second = vc;
            }
        }

        public void emitValue(Top2Acc acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }

            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }

        }
    }


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        Table table = tableEnv.fromDataStream(waterSensorStream);
// 1. 内联使用
        table
                .groupBy($("id"))
                .flatAggregate(call(Top2.class, $("vc")).as("v", "rank"))
                .select($("id"), $("v"), $("rank"))
                .execute()
                .print();
// 2. 注册后使用
//        tableEnv.createTemporaryFunction("top3", Top3.class);
//        table
//                .groupBy($("id"))
//                .flatAggregate(call("top3", $("vc")).as("v", "rank"))
//                .select($("id"), $("v"), $("rank"))
//                .execute()
//                .print();


    }


}