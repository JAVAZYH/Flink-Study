package practice.handler;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import practice.bean.OrderEvent;
import practice.bean.TxEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhangyuhang
 * \* Date: 2021/4/29
 * \* Time: 17:10
 * \订单支付实时监控
 * 需求: 来自两条流的订单交易匹配
 * 	对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了
 * 	。而往往这会来自不同的日志信息，所以我们要同时读入两条流的数据来做合并处理。
 */
public class OrderEventMonitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 1. 读取Order流
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3]));

                });
        // 2. 读取交易流
        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new TxEvent(datas[0], datas[1], Long.valueOf(datas[2]));
                });

        // 3. 两个流连接在一起
        ConnectedStreams<OrderEvent, TxEvent> orderAndTx = orderEventDS.connect(txDS);

        // 4. 因为不同的数据流到达的先后顺序不一致，所以需要匹配对账信息.  输出表示对账成功与否
        orderAndTx
                .keyBy("txId", "txId")
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    // 存 txId -> OrderEvent
                    Map<String, OrderEvent> orderMap = new HashMap<>();
                    // 存储 txId -> TxEvent
                    Map<String, TxEvent> txMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        // 获取交易信息
                        if (txMap.containsKey(value.getTxId())) {
                            out.collect("订单: " + value + " 对账成功");
                            txMap.remove(value.getTxId());
                        } else {
                            orderMap.put(value.getTxId(), value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        // 获取订单信息
                        if (orderMap.containsKey(value.getTxId())) {
                            OrderEvent orderEvent = orderMap.get(value.getTxId());
                            out.collect("订单: " + orderEvent + " 对账成功");
                            orderMap.remove(value.getTxId());
                        } else {
                            txMap.put(value.getTxId(), value);
                        }
                    }
                })
                .print();
        env.execute();

    }
}