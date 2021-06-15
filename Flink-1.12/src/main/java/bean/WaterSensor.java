package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/13
 * \* Time: 2:16 下午
 * \
 */
@Data
@NoArgsConstructor
@AllArgsConstructor


public class WaterSensor {
    //id
    private String id;
    //时间戳
    private Long ts;
    //空气高度
    private Integer vc;
}