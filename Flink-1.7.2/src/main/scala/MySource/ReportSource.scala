package MySource

import java.util.Random

import MyUtil.TimeUtil
import org.apache.flink.streaming.api.functions.source.SourceFunction

class ReportSource  extends SourceFunction[String]{
  var isRunning: Boolean = true
  val reportUin: List[String] = List("111", "222", "333", "444")
  val reportedUin: List[String] = List("1534280186", "1534280187", "1534280188", "1534280189")
  val reportedContent: List[String] = List("菜鸡", "外挂", "骂人", "逃跑")
  private val random = new Random()
  var number: Long = 0

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (isRunning) {
      val index: Int = random.nextInt(4)
      val index2: Int = random.nextInt(4)
//      ctx.collect(TimeUtil.timestampToString(System.currentTimeMillis()) +"|"
//        +23+"|"
//        +20+"|"
//        +reportedUin(random.nextInt(4))+"|"
//        +reportedContent(random.nextInt(4))+"|"
//        +reportUin(random.nextInt(4))+"|"
//        +99)
      ctx.collect(TimeUtil.timestampToString(System.currentTimeMillis()) +"|"
        +reportedUin(random.nextInt(4))+"|"
        +reportUin(random.nextInt(4))
      )
      number += 1
      Thread.sleep(500)
      if(number==100){
        cancel()
      }
    }

  }
  override def cancel(): Unit = {
    isRunning = false
  }

}