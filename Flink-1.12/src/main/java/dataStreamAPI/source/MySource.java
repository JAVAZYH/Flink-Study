package dataStreamAPI.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/14
 * \* Time: 10:25 上午
 * \
 */
public class MySource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism( 1 );

//        env.addSource(  )
    }
    public static class MySourceFunction implements SourceFunction<String>{

        private String host;
        private Integer port;
        private volatile boolean isRunning=false;


        public void run(SourceContext<String> sourceContext) throws Exception {

        }

        public void cancel() {

        }
    }
}