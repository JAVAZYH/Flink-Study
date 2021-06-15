package TSY.scene.dzlx


import com.google.gson.{Gson, JsonObject}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2021/1/5
  * \* Time: 16:30
  * \*/
object StrToJson {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    val input_DStream1: DataStream[String] = env.socketTextStream("9.134.217.5",9999)
    val LOG = LoggerFactory.getLogger(this.getClass)

    def fieldIsNull(inputStr:String): Boolean ={
      null==inputStr || inputStr=="null" || inputStr==""
    }

    val resultDS: DataStream[String] = input_DStream1
        .filter(str=>{
          val inputArr: Array[String] = str.split('|')
          val timestamp = inputArr(1 - 1)
          val game_id = inputArr(7 - 1)
          val punish_src = inputArr(5 - 1)
          val policy_id = inputArr(6 - 1)
          val ctrl_use_delay = inputArr(13 - 1)
          val punish_count = inputArr(16 - 1)
          val ctrl_delay_protect = inputArr(14 - 1)
          val ctrl_delay_start = inputArr(15 - 1)
          !fieldIsNull(timestamp)&& !fieldIsNull(game_id)&& !fieldIsNull(punish_src)&& !fieldIsNull(policy_id)&& !fieldIsNull(ctrl_use_delay)
        })
      .map(str => {
        try {
          val inputArr: Array[String] = str.split('|')
          val app_id = 5120049
          val password = "qHOeBetWqBEN"
          val timestamp = inputArr(1 - 1).toLong
          val game_id = inputArr(7 - 1).toInt
          val punish_src = inputArr(5 - 1).toInt
          val policy_id = inputArr(6 - 1).toInt
          val open_id = inputArr(4 - 1)
          val ctrl_use_delay = inputArr(13 - 1).toInt
          val punish_count = inputArr(16 - 1).toInt
          val ctrl_delay_protect = inputArr(14 - 1).toInt
          val ctrl_delay_start = inputArr(15 - 1).toInt
          val uuid =s"""cache_count_${game_id}_${punish_src}_$policy_id"""
          val resultJsonObject = new JsonObject
          val commJsonObject = new JsonObject
          val contentJsonObject = new JsonObject
          commJsonObject.addProperty("app_id", app_id)
          commJsonObject.addProperty("password", password)
          contentJsonObject.addProperty("uuid", uuid)
          contentJsonObject.addProperty("timestamp", timestamp)
          contentJsonObject.addProperty("game_id", game_id)
          contentJsonObject.addProperty("punish_src", punish_src)
          contentJsonObject.addProperty("policy_id", policy_id)
          contentJsonObject.addProperty("open_id", open_id)
          contentJsonObject.addProperty("ctrl_use_delay", ctrl_use_delay)
          contentJsonObject.addProperty("punish_count", punish_count)
          contentJsonObject.addProperty("ctrl_delay_protect", ctrl_delay_protect)
          contentJsonObject.addProperty("ctrl_delay_start", ctrl_delay_start)
          resultJsonObject.add("comm", commJsonObject)
          resultJsonObject.add("content", contentJsonObject)
          val gson = new Gson
          gson.toJson(resultJsonObject)
        }catch {
          case exception: Exception=>{
            LOG.error(s"""{$str}出错了${exception.getMessage}""")
            throw exception
          }
        }

    })
    resultDS.print()
    env.execute()
  }

}