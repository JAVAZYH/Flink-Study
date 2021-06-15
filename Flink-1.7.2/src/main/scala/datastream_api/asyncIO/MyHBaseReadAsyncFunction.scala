package datastream_api.asyncIO

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.atguigu.hbase.HBaseUtil
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.util.ExecutorUtils

class MyHBaseReadAsyncFunction extends AsyncFunction[String,String]{

//  //定义线程池
//  var executorService:ExecutorService=_
//  //定义缓存
//  var cache:Cache[String,String]=_
  //定义线程池

  lazy val executorService:ExecutorService=Executors.newFixedThreadPool(12)
  //定义缓存
  lazy val  cache:Cache[String,String]=CacheBuilder.newBuilder()
        .concurrencyLevel(12)//设置并发级别
        .expireAfterAccess(2,TimeUnit.HOURS)//设置缓存过期时间
        .maximumSize(10000)//设置缓存大小
        .build()
   var cacheNumber=0L

////  override def open(parameters: Configuration): Unit = {
//    //初始化线程池，12个线程
//    executorService=Executors.newFixedThreadPool(12)
//    //初始化缓存
//    cache=CacheBuilder.newBuilder()
//      .concurrencyLevel(12)//设置并发级别
//      .expireAfterAccess(2,TimeUnit.HOURS)//设置缓存过期时间
//      .maximumSize(10000)//设置缓存大小
//      .build()
////  }



//  override def close(): Unit = {
    //关闭线程池，缓存不关闭
//    ExecutorUtils.gracefulShutdown(100,TimeUnit.MILLISECONDS,executorService)
//  }


  override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
    resultFuture.complete(Array(""))
  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    executorService.submit(new Runnable {
      override def run(): Unit = {
        try {
          val arr: Array[String] = input.split('|')
          val columnList=Array("id","name","logtime")
          val columnFamily="c"
          val columnNmaePrefix="student"
          val tablename="tsy_test"
          /**
           * 教师和学生进行关联，通过相同的id
           * 先从缓存中读取数据，再去HBase表中查询数据，然后写入到本地缓存中
           */
          val str: String = cache.getIfPresent(arr(0))
          var student_result=""
          if(str==null||"".equals(str)){
          //111
            val rowkey=arr(0)
             student_result = HBaseUtil.getDataWithColumn(tablename, rowkey, columnFamily, columnNmaePrefix)
            cache.put(rowkey,student_result)
          }else{
            cacheNumber+=1L
          }

//          val tablename="student"
//          val cf="c"
//          //如果hbase表不存在创建表
//          if(HBaseUtil.tableExists(tablename)){
//            HBaseUtil.createTable(tablename,cf)
//          }
//          val arr: Array[String] = input.split('|')
//          val nameList=Array("id","name","logtime")
//          //采用id作为rowkey
//          val rowkey=arr(0)
//          for (index  <- arr.indices) {
//            val columnName=nameList(index)
//            val value=arr(index)时
//            HBaseUtil.putData(tablename,rowkey,cf,columnName,9999,value)
//          }
          resultFuture.complete(Array(input+student_result+cacheNumber.toString))
          ExecutorUtils.gracefulShutdown(100,TimeUnit.MILLISECONDS,executorService)
        } catch {
          case e: Exception => {
            resultFuture.complete(Array("error:" + e.printStackTrace()))
          }
        }
      }
    })

  }


}
