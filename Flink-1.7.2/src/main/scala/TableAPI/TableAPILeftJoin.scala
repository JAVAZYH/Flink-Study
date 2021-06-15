package TableAPI

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation, Types}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{QueryConfig, StreamQueryConfig, Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.FileSystem
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import MyUtil.RowUtil

object TableAPILeftJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val kdaStream: DataStream[String] = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\kdatest.txt")
    val columnList=List(List("id","STRING"),List("name","STRING"))

    val studentStream: DataStream[String] = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\student.txt")


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

    implicit  val rowType: TypeInformation[Row] = getRowTypeByCols(columnList)


    //将string流转换为row流
    def transStringStreamToRowStream(StringStream: DataStream[String]): DataStream[Row] = {
      StringStream.map(str=>{
        val arr: Array[String] = str.split('|')
        val rowResult=new Row(arr.length)
        for (pos <- arr.indices){
          rowResult.setField(pos,arr(pos))
        }
        rowResult
      })
    }


    val stu_row_stream: DataStream[Row] = transStringStreamToRowStream(studentStream)



//    val kda_table: Table = tableEnv.fromDataStream(kdaStream)

//    val path="kdatest.txt"
//    tableEnv.registerDataStream("kda_table",kdaStream)
//    val kda_table: Table = tableEnv.scan("x")
//    tableEnv.registerTable("kda_table",kda_table)

    //通过先注册表再扫描表的方式获得table
//    tableEnv.registerDataStream("student_table",stu_row_stream,'id,'name)
//    val student_table: Table = tableEnv.scan("student_table")


    //通过从流中直接转换的方式获得table
    val student_table: Table = tableEnv.fromDataStream(stu_row_stream,'id,'name)

    //table api的使用方式
    val result_table: Table = student_table
      .groupBy('id)
      .select('id,'id.count as 'count)


    import org.apache.flink.table.api.scala._
    result_table.toRetractStream[Row]
        .print("retract>>>>")



    env.execute("table>>>>>")



  }

}
