package practice.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/29
 * \* Time: 17:11
 * \订单的支付数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor

public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;
}
