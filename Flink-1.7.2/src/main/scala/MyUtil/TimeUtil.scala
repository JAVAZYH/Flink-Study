package MyUtil

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

object TimeUtil {
  /**
    *
    * @param timestamp 传入的时间戳，单位毫秒
    * @param format
    * @return 默认返回"yyyy-MM-dd HH:mm:ss" 的时间字符串
    */
  def timestampToString(timestamp: Long, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val timeFormat = new SimpleDateFormat(format)
    val timeStr = timeFormat.format(new Date(timestamp))
    timeStr
  }

  /**
    *
    * @param timeStr 传入"yyyy-MM-dd HH:mm:ss"的字符串格式
    * @param format
    * @return 返回时间戳，单位：毫秒
    */
  def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss") = {
    val timeFormat = new SimpleDateFormat(format)
    val timestamp = timeFormat.parse(timeStr).getTime
    timestamp
  }

  /**
    * 计算窗口的起始点
    * @param timeStr
    * @param offSet
    * @param windowSize
    * @return
    */
  def calTimeWindowStart(timeStr:String,offSet:Int,windowSize:Long) ={
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timestamp = timeFormat.parse(timeStr).getTime
    val result: Long = timestamp-(timestamp-offSet+windowSize)%windowSize
    timeFormat.format(new Date(result))
  }


  def calStartTime(): Long ={
    System.currentTimeMillis()
  }

  def calTotalTime(startTime:Long): String ={
    s"""该程序代码块执行总共耗时为:${(System.currentTimeMillis - startTime) }ms"""
  }

  //如果过期时间选的是今天
  def tomorrowZeroTimestampMs(now: Long, timeZone: Int): Long = now - (now + timeZone * 3600000) % 86400000 + 86400000


  def main(args: Array[String]): Unit = {
//    println(stringToTimestamp("2020-07-31 20:13:49"))
//    println(calTimeWindowStart("2020-10-29 19:18:02", 0, 300*1000))
//    println(TimeUtil.timestampToString(tomorrowZeroTimestampMs(System.currentTimeMillis(), 9)))
    println(stringToTimestamp("2020-11-04 10:30:00"))
  }
}
