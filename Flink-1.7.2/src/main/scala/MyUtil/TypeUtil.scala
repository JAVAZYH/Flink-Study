package MyUtil

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row

import scala.collection.mutable.ArrayBuffer

object TypeUtil {


  /**
    * 根据传入的string流和类型字符串，返回row类型的流
    * @param StringStream 传入的string流,string流的字段默认按照|分割
    * @param typeStr 传入的类型字符串，默认按照，分割
    * @return row类型的数据流
  */
  def transStringStreamToRowStream(StringStream: DataStream[String],typeStr:String): DataStream[Row] = {
    //对传入的类型字符串进行切割
    val typeArr: Array[String] = typeStr.split(",")
    //创建数组保存type类型信息
    val typeArrBuff: ArrayBuffer[TypeInformation[_]] =ArrayBuffer[TypeInformation[_]]()
    for (elem <- typeArr) {
      elem.toUpperCase match {
        case "BIGINT" =>typeArrBuff.append(Types.LONG)
        case "INT" => typeArrBuff.append(Types.INT)
        case "SMALLINT" => typeArrBuff.append(Types.SHORT)
        case "TINYINT" => typeArrBuff.append(Types.BYTE)
        case "BOOLEAN" => typeArrBuff.append(Types.BOOLEAN)
        case "DOUBLE" => typeArrBuff.append(Types.DOUBLE)
        case "FLOAT" => typeArrBuff.append(Types.FLOAT)
        case "SQL_TIMESTAMP" =>typeArrBuff.append(Types.SQL_TIMESTAMP)
        case _ =>typeArrBuff.append (Types.STRING)
      }
    }
    //此处隐式值是为了下面str流转为row流使用
    implicit  val input_types: TypeInformation[Row] =Types.ROW(typeArrBuff:_*)

    StringStream.map(str=>{
      val arr: Array[String] = str.split('|')
      val rowResult=new Row(arr.length)
      for (pos <- arr.indices){
        rowResult.setField(pos,arr(pos))
      }
      rowResult
    })
  }



  /**
   * 根据列格式获取对应Row的类型
   * @param columnList 列格式定义
   * @param addFlinkTime 是否在列定义末尾加上FLINK_TIME时间字段。因为AllBlue Flink作业默认所有输出末尾字段是FLINK_TIME，
   *                     如果输入的columnList里面没有包含时间字段，则需要设置为True。
   * @return
   */
  def getRowTypeByCols(columnList: List[List[String]], addFlinkTime: Boolean = false): TypeInformation[Row] = {
    var types = getTypesByCols(columnList)
    if (addFlinkTime) {
      types :+= Types.SQL_TIMESTAMP
    }
    Types.ROW(types: _*)
  }

  def getTypesByCols(columnList: List[List[String]]): List[TypeInformation[_]] = {
    val types: List[TypeInformation[_]] = columnList.map(col => {
      val allblueType = col(1)
      var colType: TypeInformation[_] = Types.STRING
      allblueType match {
        case "BIGINT" => colType = Types.LONG
        case "INT" => colType = Types.INT
        case "SMALLINT" => colType = Types.SHORT
        case "TINYINT" => colType = Types.BYTE
        case "BOOLEAN" => colType = Types.BOOLEAN
        case "DOUBLE" => colType = Types.DOUBLE
        case "FLOAT" => colType = Types.FLOAT
        case "SQL_TIMESTAMP" => colType = Types.SQL_TIMESTAMP
        case _ => colType = Types.STRING
      }
      colType
    })
    types
  }
}
