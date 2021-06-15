package datastream_api.connector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Response}
import org.apache.flink.streaming.api.scala._

object RedisTest {
  object RedisConnection extends Serializable{
    //配置使用redis
    val REDIS_HOST="ssd22.tsy.gamesafe.db"
    val REDIS_PORT=50022
    val REDIS_TIMEOUT=10000
    val REDIS_PASSWD="redisqTwhoCn"
    val config = new JedisPoolConfig()
    val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT, REDIS_PASSWD)
    lazy val jedis= pool.getResource()
  }

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input_ds: DataStream[String] = env.addSource(new MySource.StudentSource)


  }

}
// val inputDS_1: DStream[String]
// val inputDS_2: DStream[String]


//导包
//import redis.clients.jedis.{JedisPool, JedisPoolConfig, Response}
//
////操作
////
////结果数据设置为:outputDS
//// val outputDS = inputDS_1
//
//val input = inputDS_1
//
//object RedisConnection extends Serializable{
//  //配置使用redis
//  val REDIS_HOST="ssd22.tsy.gamesafe.db"
//  val REDIS_PORT=50022
//  val REDIS_TIMEOUT=10000
//  val REDIS_PASSWD="redisqTwhoCn"
//  val config = new JedisPoolConfig()
//  val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT, REDIS_PASSWD)
//  lazy val jedis= pool.getResource()
//}
//
////写入数据到redis
//input.foreachRDD(rdd=>{
//rdd.foreachPartition(logIt=>{
//val client=RedisConnection.jedis
//logIt.foreach(log=>{
//val time=log.split('|')(0).split(" ")(0)
//val user_id=log.split('|')(2)
//val game_id=log.split('|')(3)
//val punish_src=log.split('|')(6)
//val reason=log.split('|')(7)
//val key=game_id+"_"+punish_src+"_"+reason+"_"+time
////以时间为key，把qq账号作为value存储到set中
//client.sadd(key,user_id)
//client.expire(key,86400)
//})
//client.close()
//})
//})
//
//
////调整数据格式
//val output = input.transform(rdd => {
//rdd.mapPartitions(LogIt => {
//val client=RedisConnection.jedis
//LogIt.map(log => {
////对流水数据解析
//val time=log.split('|')(0).split(" ")(0)
//val user_id=log.split('|')(2)
//val game_id=log.split('|')(3)
//val punish_src=log.split('|')(6)
//val reason=log.split('|')(7)
////以游戏id+处罚源+处罚原因+日志时间作为key
//val key=game_id+"_"+punish_src+"_"+reason+"_"+time
////获取到redis中的量级统计值
//val count=client.get(key+"_count")
//println("output======="+count)
////更新redis中量级统计值的状态
//client.setex(key+"_count",86400,((if(count==null||count=="0")0 else count.toInt)+1).toString)
////获取到set集合大小作为去重统计个数
//val user_count = client.scard(key)
//log + "|" + client.get(key+"_count") + "|" + user_count
//})
//}
//)}
//)
//val  outputDS=output