package SQL

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Random

import MyUtil.{SQLUtil, TimeUtil, TypeUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{StreamQueryConfig, Table, TableEnvironment, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable.ArrayBuffer


case class report(
                   var reporttime:String,
                   var reportuin:String,
                   var reporteduin:String
                 )
case class dubious(

                    var uin:String,
                    var dubioustime:String
                  )

class StrToTime extends ScalarFunction{
  def eval(timeStr:String)={
    val format="yyyy-MM-dd HH:mm:ss"
    val timeFormat = new SimpleDateFormat(format)
    val timestamp = timeFormat.parse(timeStr).getTime
    //注意这里转换为timestamp类型的时时间戳必须要是秒级别，否则转换出错
    new Timestamp(timestamp)
  }
}


class DubiousSource  extends SourceFunction[String]{
  var isRunning: Boolean = true
  val datas: List[String] = List("1534280187", "333", "444")
  private val random = new Random()
  var number: Long = 0

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (isRunning) {
      val index: Int = random.nextInt(3)
      val value: Int = random.nextInt(10)
      if(number<=3 || (60<=number && number<=65)){
        ctx.collect("1534280186"+"|"+TimeUtil.timestampToString(System.currentTimeMillis()))
      }else{
        ctx.collect(datas(index)+"|"+TimeUtil.timestampToString(System.currentTimeMillis()))
      }
      number += 1
      Thread.sleep(10000)
//      if(number==500){
//        cancel()
//      }
    }

  }
  override def cancel(): Unit = {
    isRunning = false
  }

}


object JoinTest {
  def main(args: Array[String]): Unit = {
    //注册表环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val qconfig: StreamQueryConfig = tableEnv.queryConfig
    qconfig.withIdleStateRetentionTime(org.apache.flink.api.common.time.Time.seconds(1)
      ,org.apache.flink.api.common.time.Time.seconds(301))

    import org.apache.flink.streaming.api.scala._




    val dubious_stream: DataStream[Row] = TypeUtil.transStringStreamToRowStream(env.addSource(new DubiousSource),
      "string,string")

    val report_stream: DataStream[Row] = TypeUtil.transStringStreamToRowStream(env.addSource(new MySource.ReportSource),
      "string,string,string")

    import org.apache.flink.table.api.scala._
    val dubious_table: Table = tableEnv.fromDataStream(dubious_stream,
      SQLUtil.parseColumnName("uin,dubioustime"):_*)

    val report_table: Table = tableEnv.fromDataStream(report_stream,
      SQLUtil.parseColumnName("reporttime,reporteduin,reportuin"):_*)

    tableEnv.registerTable("dubious_table",dubious_table)
    tableEnv.registerTable("report_table",report_table)


    val strtotime=new StrToTime
    tableEnv.registerFunction("strtotime",strtotime)

//    val sql=
//      """
//        |select *
//        |from dubious_table a,report_table b
//        |where a.uin=b.reporteduin
//        |and strtotime(a.dubioustime) between  strtotime(b.reporttime) - interval '1' hour
//        |and strtotime(b.reporttime) + interval '1' hour
//      """.stripMargin

//    val sql=
//      """
//        |select * from dubious_table a
//        |right join report_table b on a.uin=b.reporteduin
//      """.stripMargin

    val sql=
      """
        |select uin,count(*) as cnt from dubious_table group by  tumble(),uin
      """.stripMargin

    val result_table: Table = tableEnv.sqlQuery(sql)

    result_table
//      .toAppendStream[Row]
        .toRetractStream[Row](qconfig)
      .print(TimeUtil.timestampToString(System.currentTimeMillis()))

    env.execute()




  }

}
