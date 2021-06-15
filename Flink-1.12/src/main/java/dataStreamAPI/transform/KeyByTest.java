package dataStreamAPI.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        DataStream<String> inputDS = env.readTextFile("input");
        DataStream<String> inputDS = env.socketTextStream("hadoop120",9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = inputDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).setParallelism(3);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = keyedStream.sum(1);

        resultDS.print();

        env.execute();
    }
}
