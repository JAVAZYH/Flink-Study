package TSY.scene.dzlx



import MyUtil.RedisUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//import akka.remote.artery.FlightRecorderReader.Log
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import redis.clients.jedis.Jedis
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.ExecutorUtils
import org.slf4j.LoggerFactory
import  scala.collection.JavaConverters._

import scala.collection.mutable
/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/12/14
  * \* Time: 10:17
  * \*/
object vocPathFilter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val inputDS: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    val keyedDS: KeyedStream[String, Int] = inputDS.keyBy(str=>{
      Random.nextInt(100)
    })

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

    class RedisAsyncFunction extends AsyncFunction[String,String] {
      lazy val executorService:ExecutorService=Executors.newFixedThreadPool(1000)

      override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
        LOG.error("timeout:" + input)
        resultFuture.complete(Array(""))
      }

      override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
        object RedisConnection extends Serializable{
          //配置使用redis
          val REDIS_HOST="ssd22.tsy.gamesafe.db"
          val REDIS_PORT=50022
          val REDIS_TIMEOUT=100000
          val REDIS_PASSWD="redisqTwhoCn"
          val config = new JedisPoolConfig()
          val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT, REDIS_PASSWD)
          lazy val jedis= pool.getResource()
        }

        executorService.submit(new Runnable {
          override def run(): Unit = {
            val client = RedisConnection.jedis
            try {
              val inputArr: Array[String] = input.split('|')
              val gameId=inputArr(gameIdPos)
              val inputPath=inputArr(archivePathPos).trim
              val pathSet: mutable.Set[String] = client.smembers(s"""${gameId}_voc_archive_path""").asScala
              val result=isFilter(inputPath,pathSet).toString
              resultFuture.complete(Array(result+"|"+input))
            } catch {
              case e: Exception => {
                println(e.getMessage)
                LOG.error("error:" + e.printStackTrace())
                throw e
              }
            }finally{
              client.close()
            }
          }
        })
      }
    }


    val result: DataStream[String] = AsyncDataStream.unorderedWait(
      keyedDS, new RedisAsyncFunction, 500, TimeUnit.SECONDS, 10000
    )
    result.print()

    env.execute()

  }

}