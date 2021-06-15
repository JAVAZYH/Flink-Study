//package SQL
//
//
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.watermark.Watermark
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.table.api.{Table, TableEnvironment, Types}
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.types.Row
//import MyUtil.{DataStreamUtil, TimeUtil}
//
//import org.apache.flink.streaming.api.scala._
//
//import scala.math.max
//
//case  class Student(
//                     var id:String,
//                     var name:String,
//                     var score:Long,
//                     var logtime:Long
//                   )
//object WindowTest {
//  def main(args: Array[String]): Unit = {
//    //注册表环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//
//    val studentStream: DataStream[String] = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\student.txt")
//
//    val stu_str_stream=studentStream.map(str=>{
//      val arr: Array[String] = str.split('|')
//      Student(arr(0),arr(1),arr(2).toLong,arr(3).toLong)
//      })
//    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Student](Time.seconds(1)) {
//    override def extractTimestamp(t: Student): Long = t.logtime*1000
//  })
//
//
////      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Row](Time.seconds(1)) {
////            override def extractTimestamp(rows: Row): Long = rows.getField(4-1).asInstanceOf[Long]*1000
////          })
//
//
////    val stu_row_stream = DataStreamUtil.transStringStreamToRowStream(studentStream)
////      .map(rows=>{
////          Student(rows.getField(0).asInstanceOf[String],
////            rows.getField(1).asInstanceOf[String],
////            rows.getField(2).asInstanceOf[Long])
////      })
//
////      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Row](Time.seconds(1)) {
////        override def extractTimestamp(rows: Row): Long = rows.getField(4-1).asInstanceOf[Long]*1000
////      })
////      .assignAscendingTimestamps(rows=>{
////         val ts: Long = rows.getField(4-1).asInstanceOf[Long]
////          ts*1000
////      })
//
//
//
//
//    //获得table的两种方式，两种方式都需要导入api隐式转换
//    import org.apache.flink.table.api.scala._
//
//
//    val student_table: Table = tableEnv.fromDataStream(stu_str_stream,'id, 'name,'score,'logtime.rowtime)
//
//     student_table.printSchema()
//
//    //定义sql语句
////    val sql=
////      s"""
////        |select * from ${student_table}
////      """.stripMargin
//
////    val sql=
////      s"""
////         |select name,count(*) as cnt from ${student_table}
////         |group by name
////      """.stripMargin
//
//
//    //gruop window
//    //注意窗口划分时flink不包含窗口结束时间的数据，比如数据是20s，窗口结束时20s，则这条数据到下一个窗口【20-30）中
////    val sql=
////      s"""
////        |select
////        |name,
////        |count(*)as cnt,
////        |tumble_start(ts,interval '10' second) as window_start,
////        |tumble_end(ts,interval '10' second) as window_end
////        |from ${student_table}
////        |group by
////        |name,
////        |tumble(ts,interval '10' second)
////      """.stripMargin
//
//    //over window，目前只支持preceding到currentrow
//    //注册表方便后续sql使用
//    tableEnv.registerTable("student",student_table)
//    //统计最近2行数据内每个学生的平均分
//    val sql=
//      """
//        |select
//        |id,
//        |name,
//        |logtime,
//        |count(id)  over w,
//        |avg(score) over w
//        |from student
//        |window w as(
//        |partition by name
//        |order by logtime
//        |rows between 2 preceding and current row
//        |)
//      """.stripMargin
//
//
//
//    val result_table: Table = tableEnv.sqlQuery(sql)
//
//    //打印执行计划
////    println(tableEnv.explain(result_table).toString)
//
//
//
//
//
//    val result_stream = result_table
//        .toAppendStream[Row]
////      .toRetractStream[Row]
//
//    result_stream.print()
//
//    env.execute()
//
//
//
//  }
//}
