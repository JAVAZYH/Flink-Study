package dataStreamAPI.sink;

import bean.WaterSensor;
import com.alibaba.fastjson.JSON;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

public class RedisSinkTest {
    public static void main(String[] args) throws Exception {
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(1000*10)
                .build();

        StreamExecutionEnvironment  env= StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromCollection(waterSensors)
                .addSink(new RedisSink<>(redisConfig, new RedisMapper<WaterSensor>() {
              /*
                key                 value(hash)
                "sensor"            field           value
                                    sensor_1        {"id":"sensor_1","ts":1607527992000,"vc":20}
                                    ...             ...
               */

                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        // 返回存在Redis中的数据类型  存储的是Hash, 第二个参数是外面的key
                        return new RedisCommandDescription(RedisCommand.HSET, "sensor");
                    }

                    @Override
                    public String getKeyFromData(WaterSensor data) {
                        // 从数据中获取Key: Hash的Key
                        return data.getId();
                    }

                    @Override
                    public String getValueFromData(WaterSensor data) {
                        // 从数据中获取Value: Hash的value
                        return JSON.toJSONString(data);
                    }
                }));

        env.execute();







    }
}
