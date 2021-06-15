package MyUtil

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
//import util.DataStreamUtil.columnList


object DataStreamUtil {

  def getTypesByCols(columnList: List[String]): List[TypeInformation[_]] = {
    val types: List[TypeInformation[_]] = columnList.map(typeStr => {
      val dataType = typeStr
      var colType: TypeInformation[_] = Types.STRING
      dataType match {
        case "bigint" => colType = Types.LONG
        case "int" => colType = Types.INT
        case "smallint" => colType = Types.SHORT
        case "tinyint" => colType = Types.BYTE
        case "boolean" => colType = Types.BOOLEAN
        case "double" => colType = Types.DOUBLE
        case "float" => colType = Types.FLOAT
        case "sql_timestamp" => colType = Types.SQL_TIMESTAMP
        case _ => colType = Types.STRING
      }
      colType
    })
    types
  }

  /**
    * string流转为row流
    * @param StringStream string流
    * @param columnList 传入的列格式,需小写
    * @return row流
    */
  implicit  val rowType: TypeInformation[Row] = getRowTypeByCols(null)
  def getRowTypeByCols(columnList: List[String], addFlinkTime: Boolean = false): TypeInformation[Row] = {
    var types = getTypesByCols(columnList)
    if (addFlinkTime) {
      types :+= Types.SQL_TIMESTAMP
    }
    Types.ROW(types: _*)
  }

    //暂时不可用
  def transStringStreamToRowStream(StringStream: DataStream[String],inputColumnList:List[String]): DataStream[Row] = {
//    val columnList: List[String] =inputColumnList
//    StringStream.map(str=>{
//      val arr: Array[String] = str.split('|')
//      val rowResult=new Row(arr.length)
//      for (pos <- arr.indices){
//        rowResult.setField(pos,arr(pos).toString)
//      }
//      rowResult
//    })
    null
  }

  def StrStreamToRowStream(StringStream: DataStream[String]): DataStream[Row] ={
//    transStringStreamToRowStream(StringStream,List[List[String]])
    null
  }


  /**
    * 将row流转为string流
    * @param rowStream
    * @return
    */
//  def transRowStreamToStringStream(rowStream: DataStream[Row]): DataStream[String] = {
////    import org.apache.flink.api.scala._
//    val stringStream = rowStream.map(rows => {
//      var string = ""
//      for (pos <- 0 until rows.getArity) {
//        if (pos == 0)
//          string += rows.getField(pos)
//        else
//          string += "|" + rows.getField(pos)
//      }
//      string
//    })
//    stringStream
//  }


  //将多个row流转为一个row流
  def transRowsStreamToRowStream(rowStream: DataStream[Row]) = {
    import org.apache.flink.api.scala._
    val result_ds: DataStream[Row] = rowStream.map(rows => {
      val result = new Row(1)
      var str = ""
      for (ele <- 0 until rows.getArity) {
        if (ele == 0) {
          str += rows.getField(ele)
        }
        else {
          str += "|" + rows.getField(ele)
        }
      }
      result.setField(0, str)
      result
    })
    result_ds
  }







}
