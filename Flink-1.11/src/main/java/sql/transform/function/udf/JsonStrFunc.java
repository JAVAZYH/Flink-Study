package sql.transform.function.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
public class JsonStrFunc extends ScalarFunction {

  // 匹配 jsonArray 格式 [{"..."}] 或 ["..."]
  private static final Pattern jsonArrayPattern =
      Pattern.compile("\\[[\\s\\n]*[{\"]+[\\s\\S]+[}\"]+[\\s\\n]*]");

  /**
   * @param fields key1,value1,key2,value2,...
   * @return JSONString: {"key1","value1","key2","value2"}
   */
  public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... fields) {
    try {
      if (fields == null || fields.length == 0 || fields.length % 2 != 0) {
        return "";
      }
      JSONObject json = new JSONObject(fields.length / 2);
      for (int i = 0; i < fields.length - 1; i += 2) {
        Object fieldKey = fields[i];
        Object fieldValue = fields[i + 1];
        String keyStr, valueStr;
        if (fieldKey == null
            || StringUtils.isEmpty(keyStr = fieldKey.toString())
            || fieldValue == null
            || StringUtils.isEmpty(valueStr = fieldValue.toString())) {
          continue;
        }

        if (valueStr.equals("{}")) {
          json.put(keyStr, new Object());
          continue;
        }
        if (valueStr.equals("[]")) {
          json.put(keyStr, new ArrayList[0]);
          continue;
        }

        // 处理 COLLECT 函数返回的数据类型 map
        if (fieldValue instanceof Map) {
          JSONArray array = new JSONArray(new ArrayList<>(((Map) fieldValue).keySet()));
          JSONArray structArray = new JSONArray(array.size());
          for (Object obj : array) {
            String objStr = obj.toString();
            // 只处理 jsonObject 格式 {"...}
            if (objStr.startsWith("{\"") && objStr.endsWith("}")) {
              structArray.add(JSONObject.parse(objStr));
              continue;
            }
            structArray.add(obj);
          }
          json.put(keyStr, structArray);
          continue;
        }

        try {
          // 只处理 jsonObject 格式 {"...}
          if (valueStr.startsWith("{\"") && valueStr.endsWith("}")) {
            json.put(keyStr, JSONObject.parse(valueStr));
            continue;
          } else if (valueStr.startsWith("[")
              && valueStr.endsWith("]")
              // 尽量避免使用正则匹配
              && jsonArrayPattern.matcher(valueStr).find()) {
            json.put(keyStr, JSONArray.parse(valueStr));
            continue;
          }
        } catch (JSONException e) {
          // 解析 json 格式出错，直接 put value
          log.error("parse json str error:{}", valueStr, e);
        }
        json.put(keyStr, fieldValue);
      }
      return json.toJSONString();
    } catch (Exception e) {
      log.error("eval error {}", StringUtils.join(fields, ","), e);
    }
    return "";
  }
}
