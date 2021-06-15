package datastream_api.broadcast;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: aresyhzhang
 * \* Date: 2021/3/30
 * \* Time: 15:05
 * \
 */


    public class RedisSink extends RichSinkFunction<Tuple2<String, String>> {

        private transient Jedis jedis;

        @Override
        public void open(Configuration config) {

            String host = "";
            String password = "";
            int port = 50000;
            int timeout = 50000;
            int db = 0;
            jedis = new Jedis(host, port, timeout);
            jedis.auth(password);
            jedis.select(db);
        }

        public void invoke(Tuple2<String, String> value, Context context) throws Exception {
            if (!jedis.isConnected()) {
                jedis.connect();
            }
            //Preservation
            jedis.set(value.f0, value.f1);
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }
