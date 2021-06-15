package TSY.etl

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer


/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/11/4
  * \* Time: 11:02
  * 实时列转行
  * \*/
object ColumnToRow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val inputDataStream: DataStream[String] = env.socketTextStream("9.134.217.5",9999)

    //需要保留的字符或字符串范围
    val retainFieldsPos="1-2"

    //需要切割的字段名，多个字段以|分隔
    val transFieldsPos="3-4"

    val input_column="account_id\tSTRING\t\ngame_id\tSTRING\t\nfea_yaw_abnoml_low_1\tBIGINT\t\nfea_yaw_abnoml_medium_1\tBIGINT\t"

    val lines: Array[String] = input_column.split('\n')
    val columnNameArr: ArrayBuffer[String] = ArrayBuffer[String]()
    for (elem <- lines) {
      val dataArr: Array[String] = elem.split("\\\t")
      columnNameArr.append(dataArr.head)
    }

    //取出保留字段位置，转换字段位置
    val retainFieldsPosArr=ArrayBuffer[Int]()
    val transFieldsPosArr=ArrayBuffer[Int]()

    val retainFieldRange: Array[String] = retainFieldsPos.split('-')
    for( index <- retainFieldRange.head.toInt to retainFieldRange.last.toInt){
      retainFieldsPosArr.append(index-1)
    }

    val transFieldRange: Array[String] = transFieldsPos.split('-')
    for( index <- transFieldRange.head.toInt to transFieldRange.last.toInt){
      transFieldsPosArr.append(index-1)
    }

    val resultDS: DataStream[String] = inputDataStream.flatMap(str => {

      val inputArr=str.split('|')
      val resultArr = new ArrayBuffer[String]()
      var retainStr = ""
      for (retainFieldIndex <- retainFieldsPosArr) {
        retainStr += inputArr(retainFieldIndex) + "|"
      }
      for (transFieldIndex <- transFieldsPosArr) {
        var transStr = ""
        val columnName = columnNameArr(transFieldIndex)
        val transFieldValue = inputArr(transFieldIndex)
        transStr = columnName + "|" + transFieldValue
        resultArr.append(retainStr + transStr)
      }
      resultArr
    })
    resultDS.print()
    env.execute()

  }

}