package sql.transform.function.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 字符串转数组
 *
 * @author daijiacheng
 * @date 2020/11/4 20:09
 */
public class StringToArrayFunc extends ScalarFunction {


  public String[] eval(String str) {
    return StringUtils.split(str, ",");
  }

  public String[] eval(String str, String separatorChars) {
    return StringUtils.split(str, separatorChars);
  }
}
