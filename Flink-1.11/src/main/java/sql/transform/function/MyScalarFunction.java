package sql.transform.function;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;

public class MyScalarFunction {
    public static class ToUpperCase extends ScalarFunction{
        public String eval(String s){
            return s.toUpperCase();
        }

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> stream = env.fromElements("hello", "aa", "Hello");

        //准备数据源和元数据
        Table table = tEnv.fromDataStream(stream, $("word"));

        //注册函数
        tEnv.createTemporaryFunction("toUpper",ToUpperCase.class);

        //注册临时表
        tEnv.createTemporaryView("t_word",table);

        tEnv.sqlQuery("select toUpper(word) word_upper from t_word").execute().print();



    }
}

