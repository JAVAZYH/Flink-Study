package SQL

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

import scala.collection.mutable.ArrayBuffer
case  class Student(
                     var id:String,
                     var name:String,
                     var logtime:String
                   )

class SplitFunction extends ScalarFunction{
  def eval(str: String): String = {
    val arr: Array[String] = str.split(',')
    val result=ArrayBuffer[String]()
    for (elem <- arr) {
      result.append('''+elem+''')
    }
    println(result.mkString(","))
    result.mkString(",")
  }
}

object UDFTest {
  def main(args: Array[String]): Unit = {





    //注册表环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //    val studentStream: DataStream[String] = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\student.txt")
    val studentStream: DataStream[String] = env.addSource(new MySource.StudentSource)
//    val  types: List[TypeInformation[_]]=List(Types.STRING,Types.STRING,Types.STRING)
//    implicit val rowType: TypeInformation[Row] = Types.ROW(types: _*)
    //将string流转换为row流
//    def transStringStreamToRowStream(StringStream: DataStream[String]): DataStream[Row] = {
//      StringStream.map(str=>{
//        val arr: Array[String] = str.split('|')
//        val rowResult=new Row(arr.length)
//        for (pos <- arr.indices){
//          rowResult.setField(pos,arr(pos))
//        }
//        rowResult
//      })
//    }



//    val student_row_stream: DataStream[Row] = transStringStreamToRowStream(studentStream)


    //获得table的两种方式，两种方式都需要导入api隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    val stu_str_stream=studentStream.map(str=>{
      val arr: Array[String] = str.split('|')
      Student(arr(0),arr(1),arr(2))
    })

    //直接从流中转换得到表
    val input_table: Table = tableEnv.fromDataStream(stu_str_stream,'id, 'name,'logtime)

//    tableEnv.registerFunction("hashCode", new HashCode(10))



    tableEnv.registerFunction("splitArr",new SplitFunction)



    val str="('三','4')"
    //定义sql语句
    val sql=
      s"""
         |select * from ${input_table}
         where '三' in (concat('(',splitArr(name),')'))
      """.stripMargin

    val result_table: Table = tableEnv.sqlQuery(sql)

//    println(tableEnv.explain(result_table).toString)


    val result_stream = result_table
      .toAppendStream[Row]

    result_stream.print()

    env.execute()



  }

}
