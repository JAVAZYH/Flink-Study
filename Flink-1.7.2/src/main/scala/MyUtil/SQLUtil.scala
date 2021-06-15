package MyUtil

import org.apache.flink.table.expressions.{Expression, ExpressionParser}

import scala.collection.mutable.ArrayBuffer

object SQLUtil {
  /**
    * 根据传入的字符串解析flink sql列名
    * @param columnStr 传入的列名字符串 例:id,name,sex
    * @return 由抽象类Expression元素组成的数组
    */
  def parseColumnName(columnStr:String): Array[Expression] ={
    val columnNameArr: Array[String] = columnStr.split(",")
    val expreArrBuff=ArrayBuffer[Expression]()
    for (elem <- columnNameArr) {
      expreArrBuff.append( ExpressionParser.parseExpression(elem))
    }
    expreArrBuff.toArray
  }

}
