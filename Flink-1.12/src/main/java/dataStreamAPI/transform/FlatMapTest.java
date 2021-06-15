package dataStreamAPI.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlatMapTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        DataSource<String> input = env.readTextFile("E:\\IdeaProject\\FlinkStudy\\Flink1.12\\src\\main\\resources\\input");
        DataSource<String> input = env.readTextFile("input");

        FlatMapOperator<String, String> WordDS = input.flatMap(new MyFlatMapFunction());

        MapOperator<String, Tuple2<String, Integer>> MapDS = WordDS.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<String, Integer>(value, 1);
                    }
                }
        );

        UnsortedGrouping<Tuple2<String, Integer>> GroupDS = MapDS.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> resultDS = GroupDS.sum(1);

        resultDS.print();

//        env.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String,String>{

        public void flatMap(String s, Collector<String> collector) throws Exception {
            //按照空格切割
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(word);
            }
        }
    }
}
