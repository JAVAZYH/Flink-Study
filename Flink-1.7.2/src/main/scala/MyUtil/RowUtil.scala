package MyUtil

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import MyUtil.FileUtil.readConfFile
import MyUtil.TypeUtil.getRowTypeByCols
import org.apache.flink.api.scala._

object RowUtil {
  //定义一个隐式值用于后续的string流转row流
  implicit  var rowType: TypeInformation[Row] = Types.ROW()

  /**
    * 根据传入的格式，把string流变为row流
    * @param path 传入的格式文件
    * @param dataStream 传入的string流
    * @return row流
    */
  def getRowStream(path:String,dataStream: DataStream[String]): DataStream[Row] = {
    //从格式文件中读取输出的列格式
    val columnsType: List[List[String]] = readConfFile(path)
    //将列格式转换为row类型的格式
    val listType: TypeInformation[Row] = getRowTypeByCols(columnsType)
    //定义一个隐式值用于后续的string流转row流
    rowType = listType

    //将string流转换为多个row流
    def transStringStreamToRowStream(StringStream: DataStream[String]): DataStream[Row] = {
      StringStream.map(str => {
        val arr: Array[String] = str.split('|')
        val rowResult = new Row(arr.length)
        for (pos <- arr.indices) {
          rowResult.setField(pos, arr(pos))
        }
        rowResult
      })
    }


        //将string流转换为单个row流
        def transStringStreamToOneRowStream(StringStream: DataStream[String]): DataStream[Row] = {
          StringStream.map(str=>{
            val rowResult=new Row(1)
            rowResult.setField(0,str)
            rowResult
          })
        }

//    返回结果
        transStringStreamToRowStream(dataStream)
      }




    /**
      * 多个row转换一个row
      *
      * @param row
      * @return
      */
    def transRowsToRow(row: Row): Row = {
      val result = new Row(1)
      var str = ""
      for (ele <- 0 until row.getArity) {
        if (ele == 0) {
          str += row.getField(ele)
        }
        else {
          str += "|" + row.getField(ele)
        }
      }
      result.setField(0, str)
      result
    }

  /**
    * 将row流转为string流
    * @param rowStream
    * @return
    */
  def transRowStreamToStringStream(rowStream: DataStream[Row]): DataStream[String] = {

    val stringStream = rowStream.map(rows => {
      var string = ""
      for (pos <- 0 until rows.getArity) {
        if (pos == 0)
          string += rows.getField(pos)
        else
          string += "|" + rows.getField(pos)
      }
      string
    })
    stringStream
  }


  //将多个row流转为一个row流
  def transRowsStreamToRowStream(rowStream: DataStream[Row]) = {
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
