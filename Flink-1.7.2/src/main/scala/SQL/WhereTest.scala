package SQL

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import MyUtil.DataStreamUtil
import org.apache.flink.api.scala._

object WhereTest {
  def main(args: Array[String]): Unit = {
    //注册表环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //    val studentStream: DataStream[String] = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\student.txt")
    val studentStream: DataStream[String] = env.addSource(new MySource.StudentSource)
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

    val student_row_stream: DataStream[Row] = transStringStreamToRowStream(studentStream)



    //获得table的两种方式，两种方式都需要导入api隐式转换
    import org.apache.flink.table.api.scala._
    import org.apache.flink.streaming.api.scala._

    //直接从流中转换得到表
    val input_table: Table = tableEnv.fromDataStream(student_row_stream,'id, 'name,'logtime)
    val filterStr="id>10"


    //定义sql语句
    val sql=
      s"""
         |select * from ${input_table}
         |where ${filterStr}
      """.stripMargin

    val result_table: Table = tableEnv.sqlQuery(sql)

//    println(tableEnv.explain(result_table).toString)


    val result_stream = result_table
      .toAppendStream[Row]

    result_stream.print()

    env.execute()



  }

}
