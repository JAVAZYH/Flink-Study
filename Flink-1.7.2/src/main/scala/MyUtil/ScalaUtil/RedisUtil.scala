package MyUtil.ScalaUtil

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
/**
  * @ Author     ：javazyh.
  * @ Date       ：Created in 2019-12-${DAT}-9:12
  * @ Description：${description}
  * @ Modified By：
  *
  * @Version: $version$
  */

object RedisUtil {

        //配置使用redis
        val REDIS_TIMEOUT = 10000
        val config = new JedisPoolConfig()
        val REDIS_HOST="9.134.217.5"
        val REDIS_PORT=6379
        val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT)

//  val REDIS_HOST = "ssd22.tsy.gamesafe.db"
//  val REDIS_PORT = 50022
//  val REDIS_PASSWORD = "redisqTwhoCn"
//    val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT,REDIS_PASSWORD)


//    val host = PropertiesUtil.getProperty("config.properties","redis.host")
//    val port = PropertiesUtil.getProperty("config.properties","redis.port").toInt
//    val password = PropertiesUtil.getProperty("config.properties","redis.password")
//    private val jedisPoolConfig = new JedisPoolConfig()
////    jedisPoolConfig.setMaxTotal(100) //最大连接数
////    jedisPoolConfig.setMaxIdle(20) //最大空闲
////    jedisPoolConfig.setMinIdle(20) //最小空闲
////    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
////    jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
////配置使用redis
//    val REDIS_TIMEOUT=10000
//    val config = new JedisPoolConfig()
//    jedisPoolConfig.setTestOnBorrow(false) //每次获得连接的进行测试
//    private val jedisPool: JedisPool = new JedisPool(jedisPoolConfig, host, port,REDIS_TIMEOUT,password)

    // 直接得到一个 Redis 的连接
    def getJedisClient: Jedis = {
        pool.getResource
    }
}

