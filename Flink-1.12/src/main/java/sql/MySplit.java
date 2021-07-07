package sql;

import org.apache.flink.table.functions.TableFunction;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: n21614
 * \* Date: 2021/6/28
 * \* Time: 10:43
 * \
 */
//@FunctionHint(output = @DataTypeHint("ROW(word string, len int)"))
    public class MySplit extends TableFunction<String> {
        // 来一个字符串, 按照逗号分割, 得到多行, 每行为这个单词和他的长度
        public  void eval(String line){
            for (String s : line.split("\\|")) {
//                collect(Row.of(s,s.length()));
                collect(s+"_"+s.length());
            }
        }
    }
