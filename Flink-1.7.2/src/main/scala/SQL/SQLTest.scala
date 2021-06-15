//package SQL
//
//import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.table.api.{Table, TableEnvironment}
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.types.Row
//import MyUtil.DataStreamUtil
//import org.apache.flink.api.scala._
//
//object SQLTest {
//  def main(args: Array[String]): Unit = {
//    //注册表环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
//
////    val studentStream: DataStream[String] = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\student.txt")
//    val studentStream: DataStream[String] = env.addSource(new MySource.StudentSource)
//
//    val stu_row_stream: DataStream[Row] = DataStreamUtil.StrStreamToRowStream(studentStream)
//
//
//    //获得table的两种方式，两种方式都需要导入api隐式转换
//    import org.apache.flink.table.api.scala._
//    import org.apache.flink.streaming.api.scala._
////    // 方式一：通过注册表的方式将把注册到tableEnv中
//    tableEnv.registerDataStream("stu_table",stu_row_stream,'id,'name,'logtime)
////    //再次扫描表获得表名
//    val student_table: Table = tableEnv.scan("stu_table")
//
//    //方式二：直接从流中转换得到表
////    val student_table: Table = tableEnv.fromDataStream(stu_row_stream,'id, 'name,'logtime)
//
//
//    //定义sql语句
////    val sql=
////      s"""
////         |select name,count(*) as cnt from ${student_table}
////         |group by name
////      """.stripMargin
//    val sql=
//      s"""
//         |select id,name,TIMESTAMP ('logtime') from stu_table
//      """.stripMargin
//
//    val result_table: Table = tableEnv.sqlQuery(sql)
//
//    println(tableEnv.explain(result_table).toString)
//
//
//    val result_stream = result_table
//      .toAppendStream[Row]
//
//    result_stream.print()
//
//    env.execute()
//
//
//
//  }
//
//}
