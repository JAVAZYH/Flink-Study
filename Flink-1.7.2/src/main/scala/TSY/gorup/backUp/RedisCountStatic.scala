package TSY.gorup.backUp

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import MyUtil.RedisUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, KeyedStream, StreamExecutionEnvironment}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer
import org.apache.flink.streaming.api.scala._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/12/8
  * \* Time: 20:09
  * \*/
object RedisCountStatic {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val inputDS: DataStream[String] = env.socketTextStream("9.134.217.5",9999)
    val keyedDS: KeyedStream[String, String] = inputDS.keyBy(_.split('|').head)

    val keyPos=""
    val valuePos=""


    class RedisAsyncFunction extends AsyncFunction[String,String] {
      lazy val executorService:ExecutorService=Executors.newFixedThreadPool(12)

      override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
        resultFuture.complete(Array(""))
      }

      override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
        executorService.submit(new Runnable {
          override def run(): Unit = {
            try {
              val client: Jedis = RedisUtil.getJedisClient
              val inputArr: Array[String] = input.split('|')
              //构造key
              val mainArr: Array[String] = keyPos.split('|')
              var key = ""
              for (ele <- mainArr) {
                key += inputArr(ele.toInt - 1)+"_"
              }

              val mainKeyArr=ArrayBuffer[String]()

              val valuePosArr: Array[String] = valuePos.split('|')
              var outputCountsField=""
              for (valuePos <- valuePosArr) {
                //2_25_8_ 4
                val mainKey=key+valuePos
                val valueData=inputArr(valuePos.toInt-1)
                client.sadd(mainKey,valueData)
                mainKeyArr.append(key+"value")
                outputCountsField=outputCountsField+"|"+client.scard(mainKey)
              }
//              client.sadd(key,)

              resultFuture.complete(Array(input))
            } catch {
              case e: Exception => {
                resultFuture.complete(Array("error:" + e.printStackTrace()))
              }
            }
          }
        })
      }
    }


    AsyncDataStream.orderedWait(
      keyedDS,new RedisAsyncFunction,101,TimeUnit.SECONDS,10
    )


  }
}