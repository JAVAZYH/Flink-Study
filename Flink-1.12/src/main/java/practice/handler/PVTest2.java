package practice.handler;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import practice.bean.UserBehavior;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/29
 * \* Time: 16:34
 * \
 */
public class PVTest2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    long count = 0;

                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        count++;
                        out.collect(count);
                    }
                })
                .print();

        env.execute();

    }

}