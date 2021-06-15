package TSY.scene.dzlx

import java.{lang, util}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.{ JedisPool, JedisPoolConfig}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/12/14
  * \* Time: 10:17
  * \*/
object vocPathFilterByWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    val archivePathPos="3".toInt-1
    val gameIdPos="2".toInt-1
    val LOG = LoggerFactory.getLogger(this.getClass)

    /**
      * 判断是否需要过滤
      * @param vocPath voc路径
      * @param pathSet voc路径集合，set集合无序需要全遍历
      * @return
      */
    def isFilter(vocPath:String,pathSet:mutable.Set[String]): Boolean ={
      //代表未配置voc路径
      if(pathSet.isEmpty){
        return false
      }
      var result=false
      val flagList: ArrayBuffer[String] = ArrayBuffer[String]()
      for (path <- pathSet) {
        flagList.append(vocPath.contains(path).toString)
      }
      if(flagList.contains("true")){
        result=true
      }else{
        result=false
      }
      result
    }

    object RedisObject extends Serializable{
//      val REDIS_TIMEOUT = 10000
//      val config = new JedisPoolConfig()
//      val REDIS_HOST="9.134.217.5"
//      val REDIS_PORT=6379
//      val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT)
      val REDIS_HOST="ssd22.tsy.gamesafe.db"
      val REDIS_PORT=50022
      val REDIS_TIMEOUT=100000
      val REDIS_PASSWD="redisqTwhoCn"
      val config = new JedisPoolConfig()
      val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT, REDIS_PASSWD)
    }

    class  vocFilterWindowFunction extends WindowFunction[String,String,String,TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[String], out: Collector[String]): Unit = {
               //每次窗口触发，这个批次一次性查询redis中voc路径数据，再循环遍历该窗口中的每条数据，给每条数据根据规则打上flag
                val client=RedisObject.pool.getResource
                val gameIdVocPath = client.smembers("dzlx_voc_archive_path").asScala
                val vocPathMap = new mutable.HashMap[String,mutable.Set[String]]()
                for (elem <- gameIdVocPath) {
                  vocPathMap.put(elem,client.smembers(elem).asScala)
                }
                val list: List[String] = input.toList
                for (input <- list) {
                  val inputArr: Array[String] = input.split('|')
                  val gameId=inputArr(gameIdPos)
                  val inputPath=inputArr(archivePathPos)
                  val pathSet=vocPathMap.getOrElse(s"""${gameId}_voc_archive_path""",mutable.Set[String]())
                  val result=isFilter(inputPath,pathSet).toString
                  out.collect(result+"|"+input)
                }
         client.close()
      }
    }

    val resultDS= input_DStream1
      .keyBy(str => {str.split('|')(gameIdPos).toString})
      .timeWindow(Time.seconds(5))
      .apply(new vocFilterWindowFunction)



//      .process(new ProcessWindowFunction[String,String,Int,TimeWindow] {
////        object RedisConnection extends Serializable{
//          val REDIS_HOST="ssd22.tsy.gamesafe.db"
//          val REDIS_PORT=50022
//          val REDIS_TIMEOUT=100000
//          val REDIS_PASSWD="redisqTwhoCn"
//          val config = new JedisPoolConfig()
//          val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT, REDIS_PASSWD)
////          val REDIS_TIMEOUT = 10000
////          val config = new JedisPoolConfig()
////          val REDIS_HOST="9.134.217.5"
////          val REDIS_PORT=6379
////          val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT)
////          lazy val jedis= pool.getResource()
////        }
////        private var client:Jedis=_
////        override def open(parameters: Configuration): Unit = {
////
////        }
//      override def process(key: Int, context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
//        val REDIS_TIMEOUT = 10000
//        val config = new JedisPoolConfig()
//        val REDIS_HOST="9.134.217.5"
//        val REDIS_PORT=6379
//        val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT)
//        lazy val client= pool.getResource()
//        val list: List[String] = elements.toList
//        for (input <- list) {
//          val inputArr: Array[String] = input.split('|')
//          val gameId=inputArr(gameIdPos)
//          val inputPath=inputArr(archivePathPos).trim
//          val pathSet: mutable.Set[String] = client.smembers(s"""${gameId}_voc_archive_path""").asScala
//          val result=isFilter(inputPath,pathSet).toString
//          out.collect(result+"|"+input)
//        }
//        client.close()
//      }
//
//    })

    resultDS.print()

    env.execute()

  }

}