package TSY.etl

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2021/1/19
  * \* Time: 19:07
  * 在flink中进行explode的操作，炸开行
  * \*/
object explodeData {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val LOG = LoggerFactory.getLogger(this.getClass)


    val inputDataStream: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    //需要切割的字段名
    val splitFieldsPos="1".toInt-1
    //需要切割的字符或字符串,字符串只支持相同字符串比如===
    val splitChar="_"



    var splitRule=""
    if(splitChar.toCharArray.length==1){
      splitRule=s"""$splitChar"""
    }else{
      splitRule=s"""[\\${splitChar.head}]{${splitChar.length}}"""
    }
    val resultDS: DataStream[String] = inputDataStream.flatMap(str => {
      val resultArr = mutable.ArrayBuffer[String]()
      val inputArr: Array[String] = str.split('|')
      val splitField = inputArr(splitFieldsPos)
      val splitArr: mutable.ArrayOps[String] = splitField.split(splitRule)
      for (elem <- splitArr) {
        inputArr.update(splitFieldsPos, elem)
        resultArr.append(inputArr.mkString("|"))
      }
      resultArr
    })


    resultDS.print()

    env.execute()


  }

}