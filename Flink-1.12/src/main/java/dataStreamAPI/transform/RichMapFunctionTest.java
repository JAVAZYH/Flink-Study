package dataStreamAPI.transform;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/14
 * \* Time: 10:14 上午
 * \
 */
public class RichMapFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism( 1 );
        env.fromElements( 1,2,3,4,5 )
                .map( new MyRichFunction()  )
                .print();
        env.execute();


    }

    public static class MyRichFunction extends RichMapFunction<Integer,Integer> {

        @Override
        public void open(Configuration parameters) throws Exception {

            System.out.println( "open开始" );
        }

        @Override
        public void close() throws Exception {
            System.out.println( "close结束" );

        }

        public Integer map(Integer value) throws Exception {
            System.out.println( "map " );
            return value*value;
        }
    }
}