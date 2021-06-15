package sql.function;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class UDTFFunctionTest {

    //一进多出
    @FunctionHint(output = @DataTypeHint("ROW(word string, len int)"))
    public static class Split extends TableFunction<Row> {
        public void eval(String line) {
            if (line.length() == 0) {
                return;
            }
            for (String s : line.split(",")) {
                // 来一个字符串, 按照逗号分割, 得到多行, 每行为这个单词和他的长度
                collect(Row.of(s, s.length()));
            }
        }
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> stream = env.fromElements("hello,aa,world", "aaa,bbbbb");

        Table table = tEnv.fromDataStream(stream, $("line"));

// 1. 注册表
        tEnv.createTemporaryView("t_word", table);
// 2. 注册函数
        tEnv.createTemporaryFunction("split", Split.class);
// 3. 使用函数
// 3.1 join
        tEnv.sqlQuery("select " +
                " line, word, len " +
                "from t_word " +
                "join lateral table(split(line)) on true").execute().print();

        System.out.println("2222222");
        // 或者
        tEnv.sqlQuery("select " +
                " line, word, len " +
                "from t_word, " +
                "lateral table(split(line))").execute().print();
// 3.2 left join
        System.out.println("22222223333");
        tEnv.sqlQuery("select " +
                " line, word, len " +
                "from t_word " +
                "left join lateral table(split(line)) on true").execute().print();













    }
}
