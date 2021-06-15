package datastream_api.asyncIO

import java.util.concurrent.{ExecutorService, Executors}

import MyUtil.TimeUtil
import com.atguigu.hbase.HBaseUtil
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}

class MyHBaseWriteAsyncFunction extends AsyncFunction[String,String]{


  lazy val executorService:ExecutorService=Executors.newFixedThreadPool(12)

  override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
    resultFuture.complete(Array(""))
  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    executorService.submit(new Runnable {
      override def run(): Unit = {
        try {
          val tablename="tsy_test"
          val cf="c"
          //如果hbase表不存在创建表`
          if(HBaseUtil.tableExists(tablename)){
            HBaseUtil.createTable(tablename,cf)
          }
          val arr: Array[String] = input.split('|')
//          val nameList=Array("school_id","school_name","school_logtime")
          val nameList=Array("student_id","student_name","student_logtime")
          //采用id+随机数作为rowkey
          val rowkey=arr(0)
          val timestamp=TimeUtil.stringToTimestamp(arr(2))
//          val timestamp=9999
          for (index  <- arr.indices) {
            val columnName=nameList(index)
            val value=arr(index)
            HBaseUtil.putData(tablename,rowkey,cf,columnName,timestamp,value)
          }
          resultFuture.complete(Array(input))
        } catch {
          case e: Exception => {
            resultFuture.complete(Array("error:" + e.printStackTrace()))
          }
        }
      }
    })

  }





}
