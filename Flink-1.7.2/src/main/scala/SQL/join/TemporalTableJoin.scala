package SQL.join

import java.io.{File, FileOutputStream, OutputStreamWriter}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/11/4
  * \* Time: 19:27
  * \*/

//测试flink sql中维表join
object TemporalTableJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
     val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)

    val sourceTableName="RatesHistory"

    def genRatesHistorySource: CsvTableSource = {

      val csvRecords = Seq(
        "rowtime ,currency   ,rate",
        "09:00:00   ,US Dollar  , 102",
        "09:00:00   ,Euro       , 114",
        "09:00:00  ,Yen        ,   1",
        "10:45:00   ,Euro       , 116",
        "11:15:00   ,Euro       , 119",
        "11:49:00   ,Pounds     , 108"
      )
      // 测试数据写入临时文件
      val tempFilePath =
        writeToTempFile(csvRecords.mkString("$"), "csv_source_", "tmp")

      // 创建Source connector
      new CsvTableSource(
        tempFilePath,
        Array("rowtime","currency","rate"),
        Array(
          Types.STRING,Types.STRING,Types.STRING
        ),
        fieldDelim = ",",
        rowDelim = "$",
        ignoreFirstLine = true,
        ignoreComments = "%"
      )
    }

    def writeToTempFile(
                         contents: String,
                         filePrefix: String,
                         fileSuffix: String,
                         charset: String = "UTF-8"): String = {
      val tempFile = File.createTempFile(filePrefix, fileSuffix)
      val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
      tmpWriter.write(contents)
      tmpWriter.close()
      tempFile.getAbsolutePath
    }


    val tableSource=genRatesHistorySource

    tableEnv.registerTableSource(sourceTableName,tableSource)


    // 注册retract sink
    val sinkTableName = "retractSink"
    val fieldNames = Array("rowtime", "currency", "rate")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING, Types.STRING)
//
//    tableEnv.registerTableSink(
//      sinkTableName,
//      fieldNames,
//      fieldTypes,
//      new MemoryRetract)







  }

}