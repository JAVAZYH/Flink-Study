package datastream_api.broadcast

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import java.io.OutputStreamWriter

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}


import scala.util.control._
import scala.io.Source.fromInputStream
import java.net.{URL, URLEncoder}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.{immutable, mutable}
import scala.util.parsing.json.JSON




/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2021/3/30
  * \* Time: 14:56
  * \*/
object dataQuality {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val LOG: Logger = LoggerFactory.getLogger(this.getClass)

        // 创建一个source
        class RuleCfgSource extends SourceFunction[mutable.Map[String, Map[String, Any]]] {

          private var running: Boolean = true
          private var updateTime = -1L
          private val allBluePath: String = ""
          private val ruleCfg: mutable.Map[String, Map[String, Any]] = mutable.Map()

          override def run(sourceContext: SourceFunction.SourceContext[mutable.Map[String, Map[String, Any]]]): Unit = {
            while (running) {
              try {
                val curTime = System.currentTimeMillis()
                if (null == ruleCfg) updateTime = -1L // 如果当前没保存规则, 那么重新获取一次全量的规则
                val updateRuleCfgStr = getRuleCfg
                val updateRuleList = JSON.parseFull(updateRuleCfgStr).get.asInstanceOf[Map[String, List[String]]].getOrElse("data", null)
                if (updateRuleList != null) {
                  val updateRuleCfgList = updateRuleList.asInstanceOf[List[Map[String, Object]]]
                  for (e <- updateRuleCfgList) {
                    // plat_id, data_source, log_type, game_id
                    println(s"new rule update: ${e.toString()}")
                    val updateStr = s"""${e.getOrElse("plat_id", "0").asInstanceOf[Double].toInt}#${e.getOrElse("data_source", "0").asInstanceOf[Double].toInt}#${e.getOrElse("log_type", "0").asInstanceOf[Double].toInt}#${e.getOrElse("game_id", "0").asInstanceOf[Double].toInt}"""
                    ruleCfg.+=(updateStr -> e)
                  }
                  updateTime = curTime / 1000
                }
              } catch {
                case _: ClassCastException => LOG.info(s"no new rule update ${updateTime}")
                case e: Exception => LOG.error(s"get rule fail: $e")
              } finally {
                if (ruleCfg != null) sourceContext.collect(ruleCfg)
                Thread.sleep(5000) // 5秒获取一次更新的配置
              }
            }
          }

          override def cancel(): Unit = {
            running = false
          }

          def encodePostParameters(data: Map[String, Any]): immutable.Iterable[String] =
            for ((name, value) <- data)
              yield URLEncoder.encode(name, "utf-8") + "=" + URLEncoder.encode(value.toString, "utf-8")

          def getRuleCfg: String = {
            val params = Map("act" -> "data_qm/get_realtime_rule_info", "login_user" -> "", "token" -> "", "updateTime" -> updateTime)
            val ruleCfg = postURL(allBluePath, params)
            ruleCfg
          }

          def postURL(url: String, parameters: Map[String, Any]): String = {
            try {
              val connection = (new URL(url)).openConnection()
              connection.setDoOutput(true)
              connection.connect()

              val postStream = new OutputStreamWriter(connection.getOutputStream)
              postStream.write(encodePostParameters(parameters).mkString("&"))
              postStream.flush()
              postStream.close()

              fromInputStream(connection.getInputStream).getLines().mkString("\n")
            } catch {
              case e: Any =>
                LOG.info("request url %s error %s\n".format(url, e))
                ""
            }
          }
        }


        // 定义广播数据的格式
        val broadcastStateDesc = new MapStateDescriptor[String, String]("rule info", classOf[String], classOf[String])
        // 注册广播流
        //context.streamExecutionEnvironment.addSource(new RuleCfgSource).setParallelism(1).print()
        val broadcastStream = env.addSource(new RuleCfgSource).setParallelism(1).broadcast(broadcastStateDesc)

        // row 转换为 string, 数据结构处理
        val stringStream = env.socketTextStream("2",9999).map(str=>{
          Tuple2(str,str)
        })

        class RealtimeQualityBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, (String, String), mutable.Map[String, Map[String, Any]], (String, String)] {
          private var timeState: ValueState[Long] = _
          private var cacheValue: ValueState[mutable.Set[String]] = _
          private var ruleCfgMap: mutable.Map[String, Map[String, Any]] = null
          private val saveRedisCount: Int = 100
          lazy val checkFailAlarms: OutputTag[(String, String, String)] = new OutputTag[(String, String, String)]("qualityAlarms")

          override def open(parameters: Configuration): Unit = {
            timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeState", classOf[Long]))
            cacheValue = getRuntimeContext.getState(new ValueStateDescriptor[mutable.Set[String]]("cacheValueState", classOf[mutable.Set[String]]))
          }

          // 处理数据
          override def processElement(in1: (String, String),
                                      readOnlyContext: KeyedBroadcastProcessFunction[String, (String, String), mutable.Map[String, Map[String, Any]], (String, String)]#ReadOnlyContext,
                                      collector: Collector[(String, String)]): Unit = {
            if (cacheValue.value() == null) {
              cacheValue.update(mutable.Set(in1._2.replace(s"${in1._1}#", "")))
            } else {
              cacheValue.update(cacheValue.value().+=(in1._2.replace(s"${in1._1}#", "")))
            }
            // 创建定时器
            val ts = readOnlyContext.timerService().currentProcessingTime()
            if (timeState.value() == 0) {
              val windowTime = ts + 1000 * 60 * 3 // 3分钟
              readOnlyContext.timerService().registerProcessingTimeTimer(windowTime)
              timeState.update(windowTime)
            }
          }

          // 收到广播数据,更新规则
          override def processBroadcastElement(in2: mutable.Map[String, Map[String, Any]],
                                               context: KeyedBroadcastProcessFunction[String, (String, String), mutable.Map[String, Map[String, Any]], (String, String)]#Context,
                                               collector: Collector[(String, String)]): Unit = {
            try {
              if (ruleCfgMap == null || !ruleCfgMap.equals(in2))
                ruleCfgMap = in2
            } catch {
              case _: NullPointerException => println(s"we got null pointer exception: $ruleCfgMap")
            }
          }

          // 定时器统一处理窗口内数据
          override def onTimer(timestamp: Long,
                               ctx: KeyedBroadcastProcessFunction[String, (String, String), mutable.Map[String, Map[String, Any]], (String, String)]#OnTimerContext,
                               out: Collector[(String, String)]): Unit = {
            // 输出需要保存到redis的数据
            val valueString = cacheValue.value().toList.take(saveRedisCount).mkString("\n")
            val currentKey = ctx.getCurrentKey
            if (ruleCfgMap.getOrElse(currentKey, null) != null) {
              // 质量检测
              var msg = ""
              var relatePerson = ""
              qualityChecker(currentKey, valueString) match {
                case (k, v) => msg = k; relatePerson = v
                case _ => LOG.error("data quality get error return")
              }
              if (msg != "") ctx.output(checkFailAlarms, (currentKey, s"$currentKey: $msg", relatePerson))
            }
            out.collect((currentKey, valueString))
            timeState.clear()
            cacheValue.clear()
          }

          def qualityChecker(currentKey: String, valueString: String): (String, String) = {

            // 这部分逻辑需要仔细测试
            val ruleInfoMap = ruleCfgMap.get(currentKey).get
            val dataDetail = ruleInfoMap.get("data_detail").get.asInstanceOf[List[List[String]]]
            val checkRule = ruleInfoMap.get("check_rule").get.asInstanceOf[List[Map[String, String]]]
            val dataId = ruleInfoMap.get("data_id").get.asInstanceOf[Double]
            val relatePerson = ruleInfoMap.get("alarm_person").get.asInstanceOf[String]
            var msg = ""
            if (dataDetail != null && checkRule != null) {
              for (rule <- checkRule) {
                println(s"check rule : $rule")
                try {
                  val colName = rule.get("col_name").get
                  val ruleType = rule.get("rule_type").get
                  val content = rule.get("content").get
                  // 获取schema
                  var colIndex = -1
                  val loop = new Breaks
                  loop.breakable {
                    for (e <- dataDetail.indices) {
                      if (colName == dataDetail(e).head) {
                        colIndex = e
                        loop.break
                      }
                    }
                  }
                  loop.breakable {
                    if (colIndex == -1) {
                      loop.break
                      println(colName)
                    } // 没有获取到对应列
                    val resultMsg = checkData(ruleType, colIndex, valueString, content, colName)
                    if(resultMsg != "") msg += s"规则检测失败$rule,data_id:$dataId, 失败内容为: $resultMsg;"
                  }
                } catch {
                  case e: Exception => println(e)
                }
              }
            }
            (msg, relatePerson)
          }

          def checkData(ruleType: String, colIndex: Int, valueString: String, content: String, colName: String): String = {
            val valueList = valueString.split("\n")
            var result = (false, "")
            ruleType match {
              case "not_null_rule" => result = checkNotNull(colIndex, valueList, colName)
              case "range_rule" => result = range_rule(colIndex, content, valueList, colName)
              case "length_rule" => result = length_rule(colIndex, content, valueList, colName)
              case "zero_rule" => result = zero_rule(colIndex, valueList, colName)
              case "unique_rule" => result = unique_rule(colIndex, valueList, colName)
              case _ => LOG.error(s"rule type is not register: $ruleType")
            }
            var msg = ""
            if (!result._1) msg = result._2
            msg
          }

          def checkNotNull(colIndex: Int, valueList: Array[String], colName: String): (Boolean, String) = {
            // 非空约束
            println("do checkNotNull rule")
            var result = true // true 为正常， false为异常
            var errorValue = ""
            val loop = new Breaks
            loop.breakable {
              for (e <- valueList) {
                if (e.split("\\|")(colIndex) == null | e.split("\\|")(colIndex) == "") {
                  result = false
                  errorValue = s"非空约束检测失败,配置字段$colName:存在null或空值: $e"
                  loop.break()
                }
              }
            }
            (result, errorValue)
          }

          def range_rule(colIndex: Int, content: String, valueList: Array[String], colName: String): (Boolean, String) = {
            // 范围约束
            println("do range_rule rule")
            var result = true // true 为正常， false为异常
            var errorValue = ""
            val loop = new Breaks
            val rangeList = content.split(",")
            val minNum = rangeList.head
            val maxNum = rangeList(1)
            loop.breakable {
              for (e <- valueList) {
                val colValue = e.split("\\|")(colIndex)
                if (("-fn" == minNum && colValue.toInt > maxNum.toInt) |
                  ("+fn" == maxNum && colValue.toInt < minNum.toInt) |
                  (colValue.toInt > maxNum.toInt || colValue.toInt < minNum.toInt)) {
                  result = false
                  errorValue = s"范围约束检测失败,配置字段$colName:不在${content}范围内的值:$colValue,字段为:$e"
                  loop.break()
                }
              }
            }
            (result, errorValue)
          }

          def length_rule(colIndex: Int, content: String, valueList: Array[String], colName: String): (Boolean, String) = {
            // 长度约束
            println("do length_rule rule")
            var result = true // true 为正常， false为异常
            var errorValue = ""
            val loop = new Breaks
            val lengthList = content.split(",")
            val minNum = lengthList.head
            val maxNum = lengthList(1)
            loop.breakable {
              for (e <- valueList) {
                val colValue = e.split("\\|")(colIndex)
                if (("-fn" == minNum && colValue.length > maxNum.toInt) |
                  ("+fn" == maxNum && colValue.length < minNum.toInt) |
                  (colValue.length > maxNum.toInt || colValue.length < minNum.toInt)) {
                  result = false
                  errorValue = s"长度约束检测失败,配置字段$colName:不在${content}范围内的值:$colValue,字段为:$e"
                  loop.break()
                }
              }
            }
            (result, errorValue)
          }

          def zero_rule(colIndex: Int, valueList: Array[String], colName: String): (Boolean, String) = {
            // 零值约束
            println("do zero_rule rule")
            var result = true // true 为正常， false为异常
            var errorValue = ""
            val loop = new Breaks
            loop.breakable(
              for (e <- valueList) {
                if (e.split("\\|")(colIndex).toInt == 0) {
                  result = false
                  errorValue = s"零值约束检测失败,配置字段$colName:存在0值 字段为 $e"
                  loop.break()
                }
              }
            )
            (result, errorValue)
          }

          def unique_rule(colIndex: Int, valueList: Array[String], colName: String): (Boolean, String) = {
            // 唯一约束
            println("do unique_rule rule")
            var result = true // true 为正常， false为异常
            val loop = new Breaks
            var errorValue = ""
            val valueSet = mutable.Set[String]()
            loop.breakable {
              for (e <- valueList) {
                val colValue = e.split("\\|")(colIndex)
                if (!valueSet.contains(colValue) && valueSet.nonEmpty) {
                  result = false
                  errorValue = s"唯一约束检测失败,配置字段$colName:存在不相同$colValue,字段为: $e"
                  loop.break()
                }
                valueSet.add(colValue)
              }
            }
            (result, errorValue)
          }
        }
        //stringStream.keyBy(_._1).connect(broadcastStream).process(new RealtimeQualityBroadcastProcessFunction).setParallelism(1).print()

        // 质量检测
        val resultStream: DataStream[(String, String)] = stringStream.keyBy(_._1).connect(broadcastStream).process(new RealtimeQualityBroadcastProcessFunction)

        // 保存到redis
        resultStream.map(data => {
          Tuple2(s"realtimeDataReview_${data._1}",data._2.asInstanceOf[String])
        })
//          .addSink(new RedisSink())


        // 侧输出流告警
        val result = resultStream.getSideOutput(new OutputTag[(String, String, String)]("qualityAlarms"))
          .keyBy(_._1)
          .timeWindow(Time.minutes(10L))
          .process(new ProcessWindowFunction[(String, String, String), (String, String, String), String, TimeWindow] {
            override def process(key: String, context: Context, elements: Iterable[(String, String, String)], out: Collector[(String, String, String)]): Unit = {
              out.collect(elements.head)
            }
          })
          .map(data => {
            val row = new Row(6)
            val dataInfo = data._1.split("#")
            row.setField(0, dataInfo(0))
            row.setField(1, dataInfo(1))
            row.setField(2, dataInfo(2))
            row.setField(3, dataInfo(3))
            row.setField(4, data._2)
            row.setField(5, data._3)
            //			val row = new Row(1)
            //			row.setField(0, s"${data._1}, ${data._2}, ${data._3}")
            row
          })

        result









  }

}