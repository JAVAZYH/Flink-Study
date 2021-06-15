package SQL
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Random

import MyUtil.{SQLUtil, TimeUtil, TypeUtil}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.{TimeCharacteristic, datastream, environment}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.JavaConverters._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.sources.{DefinedProctimeAttribute, StreamTableSource}


//class StrToTime extends ScalarFunction{
//  def eval(timeStr:String)={
//    val format="yyyy-MM-dd HH:mm:ss"
//    val timeFormat = new SimpleDateFormat(format)
//    val timestamp = timeFormat.parse(timeStr).getTime
//    //注意这里转换为timestamp类型的时时间戳必须要是秒级别，否则转换出错
//    new Timestamp(timestamp)
//  }
//}


class TimeDubiousSource  extends SourceFunction[String]{
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



class DubiousTableSource extends StreamTableSource[Row] with DefinedProctimeAttribute{
  override def getDataStream(env: environment.StreamExecutionEnvironment): datastream.DataStream[Row] = {
    env.addSource(new TimeDubiousSource).map(
      new MapFunction[String,Row] (){
        override def map(str: String) = {
            val arr: Array[String] = str.split('|')
            val rowResult=new Row(arr.length)
            for (pos <- arr.indices){
              rowResult.setField(pos,arr(pos))
            }
            rowResult
      }
    })
  }

  override def getProctimeAttribute: String = "flink_proct_time"

  override def getReturnType: TypeInformation[Row] ={
    val names=Array[String]("uin","dubioustime","flink_proct_time")
    val types=Array[TypeInformation[_]](Types.STRING,Types.STRING,Types.SQL_TIMESTAMP)

    Types.ROW(names,types)
  }

  override def getTableSchema: TableSchema = {
    val names=Array[String]("uin","dubioustime","flink_proct_time")
    val types=Array[TypeInformation[_]](Types.STRING,Types.STRING,Types.SQL_TIMESTAMP)
    new TableSchema(names,types)
  }
}

object TimeAttrTest {
  def main(args: Array[String]): Unit = {
    //注册表环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val qconfig: StreamQueryConfig = tableEnv.queryConfig
    qconfig.withIdleStateRetentionTime(org.apache.flink.api.common.time.Time.seconds(1)
      ,org.apache.flink.api.common.time.Time.seconds(301))

    import org.apache.flink.streaming.api.scala._





//    val dubious_stream: DataStream[Row] = TypeUtil.transStringStreamToRowStream(env.addSource(new DubiousSource),
//      "string,string")
//
//    val report_stream: DataStream[Row] = TypeUtil.transStringStreamToRowStream(env.addSource(new MySource.ReportSource),
//      "string,string,string")
//
//
//    val dubious_table: Table = tableEnv.fromDataStream(dubious_stream,
//      SQLUtil.parseColumnName("uin,dubioustime"):_*)
//
//
//    val report_table: Table = tableEnv.fromDataStream(report_stream,
//      SQLUtil.parseColumnName("reporttime,reporteduin,reportuin"):_*)

    tableEnv.registerTableSource("dubious_table",new DubiousTableSource)
//    tableEnv.registerTable("report_table",report_table)


//    val strtotime=new StrToTime
//    tableEnv.registerFunction("strtotime",strtotime)

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
        |select * from dubious_table
      """.stripMargin

    val result_table: Table = tableEnv.sqlQuery(sql)

    result_table
            .toAppendStream[Row]
//      .toRetractStream[Row](qconfig)
      .print(TimeUtil.timestampToString(System.currentTimeMillis()))

    env.execute()




  }

}

